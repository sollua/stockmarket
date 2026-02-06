package StockMarket;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.apache.commons.io.IOUtils;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

class AssetCorrelations {
	String asset;
	double correlationCoefficient;

	AssetCorrelations(String asset, double coeff) {
		this.asset = asset;
		correlationCoefficient = coeff;
	}
}

class DescendingOrder implements Comparator<AssetCorrelations> {
	public int compare(AssetCorrelations o1, AssetCorrelations o2) {
		AssetCorrelations i1 = (AssetCorrelations) o1;
		AssetCorrelations i2 = (AssetCorrelations) o2;
		return -new Double(i1.correlationCoefficient).compareTo(new Double(i2.correlationCoefficient));
	}
}

public class CorrelationMatrix extends Thread {
	// static ThruBreaker tb = new ThruBreaker();
	static Map<String, Map<java.util.Date, AssetStatus>> allAssetPricesMappingBasic = new HashMap<String, Map<java.util.Date, AssetStatus>>();
	static Connection conn = null;
	static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://localhost:20/mysql";
	static final String USER = "root";
	static final String PASS = "";
	static Map<String, Map<String, Double>> correlationMatrix;
	List<String> assetPile;
	static List<String> toExclude;
	static Asset UtilityAsset = new Asset();
	static List<LocalDate> priceDateRange;
	static LocalDateTime startDT;
	static LocalDateTime endDT;
	static Statement stmt;
	static List<String> onTheFlyEstimatingAssets;
	static int noOfPiles = 40;
	static ArrayList<String>[] piles = new ArrayList[noOfPiles];
	static LocalDate priceComputedTill;
	private static java.sql.Date maxDateInDailySnapshot;
	private static double deviationThreshold = -13;
	private static boolean gaugePerformanceOnly = false;
	private static boolean computeEstimatesOnTheFly = false;

	{
		new ThruBreaker();
		try {
			stmt = ThruBreaker.conn.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		correlationMatrix = new HashMap<String, Map<String, Double>>();
	}

	CorrelationMatrix(List<String> assetPile, LocalDate priceComputedTill) {
		this.assetPile = assetPile;
		CorrelationMatrix.priceComputedTill = priceComputedTill;
		// CorrelationMatrix.toExclude = listToExclude;
	}

	CorrelationMatrix() {
	}

	static double[] extractPriceVector(String assetToExtract, String majorKey, LocalDate priceComputedTill) {
		if (assetToExtract != null && majorKey != null) {
			// System.out.println("inside extractPriceVector() " + "assetToExtract: " +
			// assetToExtract + ", majorKey: " + majorKey);
			Map<java.util.Date, AssetStatus> majorMap = allAssetPricesMappingBasic.get(majorKey);
			Map<java.util.Date, AssetStatus> minorMap = allAssetPricesMappingBasic.get(assetToExtract);
			List<Date> listMajor = new ArrayList<Date>();
			for (java.util.Date d : majorMap.keySet()) {
				if (!d.after(Date.valueOf(priceComputedTill)))
					listMajor.add((Date) d);
			}
			List<Date> listMinor = new ArrayList<Date>();
			for (java.util.Date d : minorMap.keySet()) {
				if (!d.after(Date.valueOf(priceComputedTill)))
					listMinor.add((Date) d);
			}
			List<Date> listIntersection = new ArrayList<Date>();
			for (Date d : listMajor) {
				if (listMinor.contains(d)) {
					listIntersection.add(d);
				}
			}
			Collections.sort(listIntersection);
			Iterator<Date> iterDates = listIntersection.iterator();
			Date dd;
			int i = 0;
			double[] valuesPlainDouble = new double[listIntersection.size()];
			while (iterDates.hasNext()) {
				dd = iterDates.next();
				valuesPlainDouble[i] = minorMap.get(dd).closePrice;
				// System.out.println(a + ": " + dd + ", " + values1PlainDouble[i]);
				i++;
			}
			return valuesPlainDouble;
		} else {
			return null;
		}
	}

	static double computeCorrelation(String a, String b, LocalDate priceComputedTill, String correlationType) {
		double[] values1PlainDouble = extractPriceVector(a, b, priceComputedTill);
		double[] values2PlainDouble = extractPriceVector(b, b, priceComputedTill);

		if (values1PlainDouble.length == values2PlainDouble.length && values1PlainDouble.length > 1) {
			if (correlationType.equals("kendall")) {
				return UtilityAsset.KendallCorrelation(values1PlainDouble, values2PlainDouble);
			} else if (correlationType.equals("pearson")) {
				return UtilityAsset.PearsonCorrelation(values1PlainDouble, values2PlainDouble);
			} else {
				return UtilityAsset.PearsonCorrelation(values1PlainDouble, values2PlainDouble);
			}
		} else {
			return -1;
		}
	}

	static void perserveCorrelationMatrix(String majorKey, Map<String, Double> tinyMap, LocalDate priceComputedTill) {
		String deviationAssertion = null;
		String stockName;
		if (allAssetPricesMappingBasic.get(majorKey).get(ThruBreaker.maxDateInDailySnapshot) == null
				|| allAssetPricesMappingBasic.get(majorKey).get(ThruBreaker.maxDateInDailySnapshot).stockName == null) {
			stockName = "";
		} else {
			stockName = allAssetPricesMappingBasic.get(majorKey).get(ThruBreaker.maxDateInDailySnapshot).stockName;
		}
		if (Date.valueOf(priceComputedTill).after(maxDateInDailySnapshot)) {
			deviationAssertion = "insert into matrix_correlation (asset, name, computing_date, component_1,"
					+ "component_2, component_3, component_4,component_5,component_6, component_7,"
					+ "component_8, component_9, component_10, component_1_corrCoefficient,"
					+ "component_2_corrCoefficient, component_3_corrCoefficient,component_4_corrCoefficient,"
					+ "component_5_corrCoefficient, component_6_corrCoefficient,component_7_corrCoefficient,"
					+ "component_8_corrCoefficient, component_9_corrCoefficient,component_10_corrCoefficient,"
					+ "beta_0, beta_1, beta_2, beta_3, beta_4, beta_5, beta_6, beta_7,"
					+ "beta_8, beta_9, beta_10, estimate_upToComputeDate, residual_upToComputeDate,"
					+ "price_upToComputeDate, price_estimate_deviation, price_estimate_deviation_numeric, realtime"
					+ ") values " + "('" + majorKey + "', '" + stockName + "', '" + priceComputedTill + "' , ";
		} else {
			deviationAssertion = "insert into matrix_correlation (asset, name, computing_date," + "component_1,"
					+ "component_2," + "component_3," + "component_4," + "component_5," + "component_6,"
					+ "component_7," + "component_8," + "component_9," + "component_10,"
					+ "component_1_corrCoefficient," + "component_2_corrCoefficient," + "component_3_corrCoefficient,"
					+ "component_4_corrCoefficient," + "component_5_corrCoefficient," + "component_6_corrCoefficient,"
					+ "component_7_corrCoefficient," + "component_8_corrCoefficient," + "component_9_corrCoefficient,"
					+ "component_10_corrCoefficient," + "beta_0," + "beta_1," + "beta_2," + "beta_3," + "beta_4,"
					+ "beta_5," + "beta_6," + "beta_7," + "beta_8," + "beta_9," + "beta_10,"
					+ "estimate_upToComputeDate," + "residual_upToComputeDate," + "price_upToComputeDate,"
					+ "price_estimate_deviation, " + "price_estimate_deviation_numeric" + ") values " + "('" + majorKey
					+ "', '" + stockName + "', '" + priceComputedTill + "' , ";
		}
		List<AssetCorrelations> assetCorrelationCoefficients;
		assetCorrelationCoefficients = new LinkedList<AssetCorrelations>();
		for (String minorKey : tinyMap.keySet()) {
			assetCorrelationCoefficients.add(new AssetCorrelations(minorKey, tinyMap.get(minorKey)));
		}
		System.out.println("correlation matrix routine: " + majorKey + "'s insertion sql is: " + deviationAssertion);

		Collections.sort(assetCorrelationCoefficients, new DescendingOrder());
		String componentAssetCodeVector[] = new String[10];
		double componentAssetCoeffientsVector[] = new double[10];

		int i = 0;
		for (AssetCorrelations d : assetCorrelationCoefficients) {
			if (assetCorrelationCoefficients.indexOf(d) < 1) {
				continue;
			}
			if (assetCorrelationCoefficients.indexOf(d) > 10) {
				break;
			}
			i++;
			componentAssetCodeVector[i - 1] = d.asset;
			deviationAssertion = deviationAssertion.concat("'" + String.valueOf(d.asset) + "',");
		}

		i = 0;
		for (AssetCorrelations d : assetCorrelationCoefficients) {
			if (assetCorrelationCoefficients.indexOf(d) < 1) {
				continue;
			}
			if (assetCorrelationCoefficients.indexOf(d) > 10) {
				break;
			}
			i++;
			componentAssetCoeffientsVector[i - 1] = d.correlationCoefficient;
			deviationAssertion = deviationAssertion.concat("'" + String.valueOf(d.correlationCoefficient) + "',");
		}

		double[] Y = extractPriceVector(majorKey, majorKey, priceComputedTill);

		double[][] X = new double[Y.length][componentAssetCodeVector.length];
		double tempY[] = null;
		for (int j = 0; j < componentAssetCodeVector.length; j++) {
			tempY = extractPriceVector(componentAssetCodeVector[j], majorKey, priceComputedTill);
			System.out.println("correlation matrix routine: assetToEstimate :" + majorKey);
			System.out.println("correlation matrix routine: " + componentAssetCodeVector[j]);
			System.out.println("correlation matrix routine: " + componentAssetCodeVector[j] + "'s k(tempY.length): "
					+ tempY.length);
			for (int k = 0; k < tempY.length; k++) {
				X[k][j] = tempY[k];
			}
		}
		OLSMultipleLinearRegression ols = new OLSMultipleLinearRegression();
		double[] beta = null;
		try {
			ols.newSampleData(Y, X);
			beta = ols.estimateRegressionParameters();
		} catch (org.apache.commons.math3.linear.SingularMatrixException
				| org.apache.commons.math3.exception.NoDataException singularOrNoDataEx) {
			throw singularOrNoDataEx;
		}
		System.out.println("correlation matrix routine: (betas) ");
		for (i = 0; i < beta.length; i++) {
			deviationAssertion = deviationAssertion.concat("'" + String.valueOf(beta[i]) + "', ");
			System.out.println("correlation matrix routine: " + beta[i]);
		}
		System.out.println("correlation matrix routine: " + majorKey + "'s insertion sql is: " + deviationAssertion);

		double[] residuals = ols.estimateResiduals();
		double YHat[] = new double[Y.length];

		for (int j = 0; j < Y.length; j++) {
			for (i = 0; i < beta.length; i++) {
				if (i == 0) {
					YHat[j] = beta[i];
				} else {
					YHat[j] = YHat[j] + beta[i] * X[j][i - 1];
				}
			}
		}

		// System.out.println(majorKey + "'s YHat[" + j + "]: " + (YHat[j])); }

		/*
		 * for (i = 0; i < beta.length; i++) { if (i == 0) { YHat[YHat.length - 1] =
		 * beta[i]; } else { YHat[YHat.length - 1] = YHat[YHat.length - 1] + beta[i] *
		 * X[YHat.length - 1][i - 1]; } }
		 */
		System.out.println(
				"correlation matrix routine: " + majorKey + "'s last YHat(estimation): " + (YHat[YHat.length - 1]));
		System.out.println("correlation matrix routine: " + majorKey + "'s insertion sql is: " + deviationAssertion);
		double priceToEstimateDeviation;
		i = 0;
		if (allAssetPricesMappingBasic.get(majorKey).get(Date.valueOf(priceComputedTill)) != null) {
			priceToEstimateDeviation = (residuals[residuals.length - 1])
					/ allAssetPricesMappingBasic.get(majorKey).get(Date.valueOf(priceComputedTill)).closePrice * 100;
			deviationAssertion = deviationAssertion
					.concat("'" + YHat[YHat.length - 1] + "','" + residuals[residuals.length - 1] + "','"
							+ allAssetPricesMappingBasic.get(majorKey).get(Date.valueOf(priceComputedTill)).closePrice
							+ "','" + String.format("%1$,.2f", priceToEstimateDeviation).concat("%") + "','"
							+ priceToEstimateDeviation + "'");
		} else {
			while (allAssetPricesMappingBasic.get(majorKey).get(Date.valueOf(priceComputedTill.minusDays(i))) == null) {
				i++;
			}
			priceToEstimateDeviation = (residuals[residuals.length - 1]) / allAssetPricesMappingBasic.get(majorKey)
					.get(Date.valueOf(priceComputedTill.minusDays(i))).closePrice * 100;
			deviationAssertion = deviationAssertion
					.concat("'" + YHat[YHat.length - 1] + "','" + residuals[residuals.length - 1] + "','"
							+ allAssetPricesMappingBasic.get(majorKey)
									.get(Date.valueOf(priceComputedTill.minusDays(i))).closePrice
							+ "','" + String.format("%1$,.2f", priceToEstimateDeviation).concat("%") + "','"
							+ priceToEstimateDeviation + "'");
		}

		if ((Date.valueOf(priceComputedTill)).after(maxDateInDailySnapshot)) {
			deviationAssertion = deviationAssertion.concat(",'Y');");
		} else {
			deviationAssertion = deviationAssertion.concat(");");
		}

		try {
			System.out.println("correlation matrix routine: deviationAssertion: " + deviationAssertion);
			stmt.executeQuery(deviationAssertion);
			System.out.println("correlation matrix routine: " + majorKey + " inserted.");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void run() {
		System.out.println("correlation matrix routine: " + this.getName() + ": new correlation matrix is running...");
		/*
		 * try { Thread.sleep(1000); } catch (InterruptedException e) {
		 * e.printStackTrace(); }
		 */
		Map<String, Double> minorMap = null;

		double corr;
		if (toExclude == null) {
			toExclude = new LinkedList<String>();
		}
		for (String majorKey : assetPile) {
			if (!toExclude.contains(majorKey)) {
				minorMap = new HashMap<String, Double>();
				if (majorKey != null) {
					for (String minorKey : allAssetPricesMappingBasic.keySet()) {
						if (minorKey != null) {
							corr = CorrelationMatrix.computeCorrelation(minorKey, majorKey, priceComputedTill,
									"pearson");
							minorMap.put(minorKey, corr);
						}
					}
					correlationMatrix.put(majorKey, minorMap);
					try {
						perserveCorrelationMatrix(majorKey, minorMap, priceComputedTill);
					} catch (org.apache.commons.math3.linear.SingularMatrixException singularEx) {
						System.out.println(
								"correlation matrix routine: SingularMatrixException thrown on asset " + majorKey);
						continue;
					} catch (org.apache.commons.math3.exception.NoDataException noDataEx) {
						System.out.println("correlation matrix routine: NoDataException thrown on asset " + majorKey
								+ ", might be newly issued asset. ");
						continue;
					} catch (org.apache.commons.math3.exception.MathIllegalArgumentException illegalArgumentEx) {
						System.out.println(
								"correlation matrix routine: correlation matrix routine: MathIllegalArgumentException thrown on asset "
										+ majorKey + ", being " + illegalArgumentEx.getStackTrace());
						System.out.println("correlation matrix routine: Might be newly issued asset. ");
						continue;
					} catch (Exception ex) {
						System.out.println("correlation matrix routine: " + ex.getCause()
								+ " exception occurred, continue to next asset.");
						continue;
					}
				}
			}
		}
		System.out.println(
				"correlation matrix routine: " + this.getName() + ": correlation matrix computation finished...");
		/*
		 * if (CorrelationMatrix.gaugePerformanceOnly == true) { File file = new
		 * File("/Users/pengzhou/Desktop/out/dense_assets_report_" + LocalDateTime.now()
		 * + ".txt"); File file1 = new File("/Users/pengzhou/Desktop/out/assets_" +
		 * LocalDateTime.now() + ".txt"); FileWriter fw = null, fw1 = null; try { fw =
		 * new FileWriter(file); fw1 = new FileWriter(file1); } catch (IOException e) {
		 * // TODO Auto-generated catch block e.printStackTrace(); } String assetPoolSql
		 * =
		 * "select asset, close_price max_close, first_admitted_close, first_admitted, "
		 * + "d.交易日期 max_trade_date, DATEDIFF(d.交易日期, first_admitted) to_max_duration,"
		 * +
		 * " (close_price-first_admitted_close)/first_admitted_close*100 acc_change from   \n"
		 * +
		 * "   (select asset, first_admitted , close_price first_admitted_close, 交易日期,last_admitted from\n"
		 * + " (select 股票代码, 收盘价 close_price, 交易日期 from daily_snapshot) a join \n" +
		 * " (select  asset, min(computing_date) first_admitted, max(computing_date) last_admitted "
		 * + "from matrix_correlation where price_estimate_deviation_numeric<=" +
		 * deviationThreshold + " and computing_date>='2024-10-01'\n" +
		 * " group by asset) c \n" +
		 * "on a.股票代码= c.asset and a.交易日期= c.first_admitted) k \n" + "join \n" +
		 * " (select 股票代码, 收盘价 close_price, 交易日期 from daily_snapshot where  交易日期 >='2024-10-01') d \n"
		 * +
		 * " on k.asset = d.股票代码 and d.交易日期 >= k.first_admitted order by asset, close_price desc;  "
		 * ; try { Date firstAdmitted = null; double maxClose = 0; double
		 * firstAdmittedClose = 0; Date maxCloseTradeDate = null; int duration = 0;
		 * double accChange = 0; String currentAsset = "Foo"; rs =
		 * stmt.executeQuery(assetPoolSql); System.out.
		 * println("asset, asset(short), first_admitted, first_admitted_close, max_close, "
		 * + "max_trade_date, to_max_duration(days), accumulated_change" + "\n"); fw.
		 * write("asset, asset(short), first_admitted, first_admitted_close, max_close, "
		 * + "max_trade_date, to_max_duration(days), accumulated_change" + "\n"); while
		 * (rs.next()) { if (!currentAsset.equals(rs.getString("asset"))) { currentAsset
		 * = rs.getString("asset"); asset = rs.getString("asset"); firstAdmitted =
		 * rs.getDate("first_admitted"); maxClose = rs.getDouble("max_close");
		 * firstAdmittedClose = rs.getDouble("first_admitted_close"); maxCloseTradeDate
		 * = rs.getDate("max_trade_date"); duration = rs.getInt("to_max_duration");
		 * accChange = rs.getDouble("acc_change"); String output = asset + ", " +
		 * asset.substring(2) + ", " + firstAdmitted + ", " + firstAdmittedClose + ", "
		 * + maxClose + ", " + maxCloseTradeDate + ", " + duration + ", " +
		 * String.format("%1$,.2f", accChange) + "%"; System.out.println(output);
		 * fw.write(output + "\n"); } } } catch (SQLException e) { // TODO
		 * Auto-generated catch block e.printStackTrace(); } catch (IOException e) { //
		 * TODO Auto-generated catch block e.printStackTrace(); } }
		 */
	}

	public static void main(String args[]) {
		LocalDateTime startDT = LocalDateTime.now();
		new CorrelationMatrix();
		ResultSet rs;
		String asset = null;
		LocalDate priceStart = LocalDate.of(2024, 1, 1);
		LocalDate priceTill = LocalDate.now();
		CorrelationMatrix[] threads = null;
		priceDateRange = ThruBreaker.setPriceDateRange(priceStart, priceTill);
		allAssetPricesMappingBasic = ThruBreaker.setAllAssetPricesMappingBasic(priceStart, priceTill,
				ThruBreaker.conn, stmt);
		for (LocalDate d : priceDateRange) {
			if (d.isAfter(LocalDate.of(2024, 11, 19)) && !d.isAfter(LocalDate.now())) {

				if (computeEstimatesOnTheFly == true) {
					priceComputedTill = LocalDate.now();
				} else {
					priceComputedTill = d;
					// priceComputedTill = LocalDate.of(2024, 11, 19);
				}

				String selectmaxDateInDailySnapshot = "select max(交易日期) as maxDate from daily_snapshot;";
				maxDateInDailySnapshot = null;
				ResultSet rs_maxDate;
				try {
					rs_maxDate = stmt.executeQuery(selectmaxDateInDailySnapshot);
					while (rs_maxDate.next()) {
						maxDateInDailySnapshot = rs_maxDate.getDate("maxDate");
					}
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				int i = 0, j = 0;
				if (gaugePerformanceOnly == false) {
					ArrayList<String>[] piles = new ArrayList[noOfPiles];
					toExclude = new LinkedList<String>();
					try {
						String deleteExistingInTheFlyEstimates = "delete from matrix_correlation where realtime='Y';";
						stmt.executeQuery(deleteExistingInTheFlyEstimates);
						String toExcludeAssets = "select * from matrix_correlation where computing_date='"
								+ priceComputedTill.toString() + "'";

						rs = stmt.executeQuery(toExcludeAssets);
						while (rs.next()) {
							asset = rs.getString("asset");
							toExclude.add(asset);
						}

						if ((Date.valueOf(priceComputedTill)).after(maxDateInDailySnapshot)
								&& computeEstimatesOnTheFly == false) {
							Date realtimeTradeDate = new java.sql.Date(-1);// to initialize a java.sql.Date as
																			// 1970/01/01
																			// (realtimeprice table's trade_date
																			// non-null
																			// check).

							String checkRealtimePriceTradeDate = "select max(trade_date) max_trade_date from realtimeprice;";
							String onTheFlyEstimatingAssetsSql = "select distinct asset from matrix_correlation where price_estimate_deviation_numeric<='"
									+ deviationThreshold + "' and computing_date>='2024-10-01'";
							try {
								rs = stmt.executeQuery(checkRealtimePriceTradeDate);
								while (rs.next()) {
									realtimeTradeDate = rs.getDate("max_trade_date");
								}
								if (!priceComputedTill.isBefore(realtimeTradeDate.toLocalDate())) {
									System.out.println(
											"correlation matrix routine: valid realtime-price data, proceed on-the-fly estimating...");
								} else {
									System.out.println(
											"correlation matrix routine: invalid realtime-price data, exit on-the-fly estimating...");
									return;
								}
								rs = stmt.executeQuery(onTheFlyEstimatingAssetsSql);
								onTheFlyEstimatingAssets = new LinkedList<String>();
								while (rs.next()) {
									asset = rs.getString("asset");
									onTheFlyEstimatingAssets.add(asset);
								}
							} catch (SQLException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							for (String majorKey : onTheFlyEstimatingAssets) {
								if (i % Math.ceil(onTheFlyEstimatingAssets.size() / noOfPiles + 1) == 0) {
									List<String> assetPile = new ArrayList<String>();
									piles[j] = (ArrayList<String>) assetPile;
									j++;
								}
								piles[j - 1].add(majorKey);
								i++;
							}
						} else {
							for (String majorKey : allAssetPricesMappingBasic.keySet()) {
								if (i % Math.ceil(allAssetPricesMappingBasic.keySet().size() / noOfPiles + 1) == 0) {
									List<String> assetPile = new ArrayList<String>();
									piles[j] = (ArrayList<String>) assetPile;
									j++;
								}
								piles[j - 1].add(majorKey);
								i++;
							}
						}
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					threads = new CorrelationMatrix[piles.length];
					for (int pileIndex = 0; pileIndex < piles.length; pileIndex++) {
						CorrelationMatrix cm = new CorrelationMatrix(piles[pileIndex], priceComputedTill);
						threads[pileIndex] = cm;
						cm.start();
					}
				}

				int livingThreads;
				while (true) {
					livingThreads = 0;
					for (j = 0; j < threads.length; j++) {
						livingThreads += (threads[j].isAlive() ? 1 : 0);
					}
					if (livingThreads == 0) {
						break;
					}
				}
				LocalDateTime endDT = LocalDateTime.now();
				System.out.println("correlation matrix routine: Ending correlation computing at " + endDT + ", cost "
						+ startDT.until(endDT, ChronoUnit.SECONDS) + " seconds.");
			}
		}
	}
}
