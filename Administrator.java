package StockMarket;

import java.lang.reflect.Type;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class Administrator extends Thread {
	Administrator(Set<String> codeSet) {
		this.codeSet = codeSet;
	}

	Administrator() {
	}

	public static double round(double value, int places) {
		if (places < 0)
			throw new IllegalArgumentException();

		long factor = (long) Math.pow(10, places);
		value = value * factor;
		long tmp = Math.round(value);
		return (double) tmp / factor;
	}

	static ZoneId defaultZoneId = ZoneId.systemDefault();

	class DescendingOrder implements Comparator<Asset> {
		public int compare(Asset o1, Asset o2) {
			Asset i1 = (Asset) o1;
			Asset i2 = (Asset) o2;
			return -new Double(i1.dailyChange).compareTo(new Double(i2.dailyChange));
		}
	}

	private Set<String> codeSet;

	public void PCARun() throws SQLException {
		ResultSet rs, rs0, rs1, rs2;
		Statement stmt;
		new ThruBreaker();

		List<LocalDate> days = ThruBreaker.setPriceDateRange(LocalDate.of(2024, 1, 01), LocalDate.now());
		try {
			File file = new File("/Users/pengzhou/Desktop/out/gains_report_" + LocalDateTime.now() + ".txt");
			FileWriter fww = null;
			fww = new FileWriter(file);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String asset;
		String name = null, originalName = null, exchange = null, originalAsset = null;
		stmt = ThruBreaker.conn.createStatement();
		rs0 = stmt.executeQuery("select * from listing where stock_code not like ('bj%');");
		while (rs0.next()) {
			exchange = rs0.getString("exchange");
			asset = exchange.concat(rs0.getString("stock_code"));
			originalName = rs0.getString("stock_name");
			originalAsset = asset;

			int correlatedSlotNum = 10;
			// asset="sh600965";
			// asset="sh688098";
			// asset="sh688345";
			// asset="sz002909";
			// asset="sz002524";
			// asset="sh600800";
			Administrator ut = new Administrator();
			List<Double> perf = new LinkedList<Double>();
			String seedAsset, seedName = null;
			ListIterator<LocalDate> li = days.listIterator();
			li.next();
			try {
				while (li.hasNext()) {
					LocalDate d = (LocalDate) li.next();
					String sql = "   select asset,component_1,\n" + "    component_2,\n" + "    component_3,\n"
							+ "    component_4,\n" + "component_5,\n" + "    component_6,\n" + "    component_7,\n"
							+ "    component_8,\n" + "    component_9,\n" + "    component_10\n"
							+ "    from matrix_correlation \n" + "   where asset= '" + asset + "' and computing_date='"
							+ days.get(days.lastIndexOf(d) - 1) + "';";
					String sql_6 = "   select asset,component_1,\n" + "component_2,\n" + "component_3,\n"
							+ "    component_4,\n" + "component_5\n" + "    from matrix_correlation \n"
							+ "   where asset= '" + asset + "' and computing_date='" + days.get(days.lastIndexOf(d) - 1)
							+ "';";

					seedAsset = asset;
					rs = stmt.executeQuery(sql);
					// System.out.println("asset: "+asset);
					double sum = 0, avg = 0;
					double max = -1;
					String component;
					List<Asset> cps = new LinkedList<Asset>();
					String cp;
					while (rs.next()) {
						for (int i = 1; i <= correlatedSlotNum + 1; i++) {
							cp = rs.getString(i);
							Asset ast = new Asset();
							ast.stockCode = cp;
							ast.notes = "component_" + (i - 1);
							// System.out.println("component_"+i+": "+ cp);
							rs1 = stmt.executeQuery("select 股票代码, 股票名称, 涨跌幅 from daily_snapshot where 交易日期='" + d
									+ "' and 股票代码 ='" + cp + "';");
							while (rs1.next()) {
								ast.stockCode = component = rs1.getString("股票代码");
								ast.stockName = rs1.getString("股票名称");
								if (component.equals(seedAsset)) {
									seedName = ast.stockName;
								}

								double change = rs1.getDouble("涨跌幅");
								/// System.out.println("change_"+i+": "+ change);
								// sum += change;
								ast.dailyChange = (float) change;
								if (max < change) {
									max = change;
									asset = component;
									name = ast.stockName;
								}
							}
							cps.add(ast);
						}
						cps.sort(ut.new DescendingOrder());
						int j = 0;
						for (Asset cpi : cps) {
							if (j <= correlatedSlotNum) {
								sum += cpi.dailyChange;
								j++;
							}
						}
						avg = sum / (correlatedSlotNum + 1);
						j = 0;

						System.out.println("on: " + days.get(days.lastIndexOf(d) - 1) + ", " + seedName
								+ " was correlated with (with " + correlatedSlotNum
								+ " most next day gains), with next day to be " + d + ":");
						for (Asset cpi : cps) {
							if (j <= correlatedSlotNum) {
								System.out.println(cpi.stockName + "(" + cpi.stockCode + ", " + cpi.notes + ")" + ": "
										+ cpi.dailyChange);
								j++;
							}
						}
						System.out.println("so " + name + "(" + asset + ")" + " was chosen.");
						System.out.println(d + "'s avg gain is: " + avg);
						rs2 = stmt.executeQuery(
								"  select a.股票代码, 股票名称, 收盘价, last_close, (收盘价-last_close)/last_close index_change from \n"
										+ "   (select 股票代码, 股票名称, 收盘价, 涨跌幅 from daily_snapshot where 交易日期='" + d
										+ "' and 股票代码 ='sh000001') a\n" + "   join \n"
										+ "   (select  股票代码, 收盘价 last_close from daily_snapshot where 交易日期='"
										+ days.get(days.lastIndexOf(d) - 1) + "' and 股票代码 ='sh000001') b\n"
										+ "   on a.股票代码=b.股票代码;");
						while (rs2.next()) {
							System.out.println(d + "'s index change is: " + rs2.getDouble("index_change"));
						}

						perf.add(avg);
					}
				}

				double compounding = 1;
				for (double d : perf) {
					compounding = compounding * (1 + d);
				}
				System.out.println("with " + perf.size() + " trading days, " + originalName + "(" + originalAsset + ")"
						+ "'s perf compounding: " + compounding);

			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static void morningBuyingTest(boolean interactive, String upOrDown, LocalDate d) throws SQLException {
		new ThruBreaker();
		ThruBreaker.setPriceDateRange(d, LocalDate.now());
		List<LocalDate> pl = ThruBreaker.priceDateRange;

		double acc = 1;
		for (int s = 0; s < ThruBreaker.priceDateRange.size(); s++) {
			ListIterator<LocalDate> li = pl.listIterator(pl.size() - s);
			int i = 0;
			LocalDate range = null;
			while (i <= 5) {
				i++;
				// System.out.println("i: "+ i);
				// System.out.println(i<10);
				if (li.hasPrevious())
					range = li.previous();
			}
			LocalDate prev = null;
			li = pl.listIterator(pl.size() - s);
			li.previous();
			if (li.hasPrevious()) {
				prev = li.previous();
			}
			li.next();
			LocalDate next = li.next();
			LocalDate nextNext;

			String ind = null;
			String upCappingSql, downCappingSql;
			if (interactive) {
				Scanner scanner = new Scanner(System.in);
				System.out.print("Enter: ");
				ind = scanner.nextLine();
				System.out.println("on " + prev + ", Looking at: " + ind);
			}
			if (li.hasNext()) {
				// li.next();
				nextNext = li.next();
			}
			String testDownwardSql = "select avg(涨跌幅) gain from daily_snapshot where 股票代码 in (\n"
					+ "   select 股票代码 from daily_snapshot where 股票代码 in  \n" + "   (select a.stock_code from \n"
					+ "    (select stock_code, name, select_date, description, open, close, 'downward' as source \n"
					+ "     from downward where select_date>='" + range.toString() + "' and select_date<= ' " + prev
					+ "' and stock_code not like '%688%') a \n"
					+ "    join (select stock_code, max(select_date) as select_date from downward where stock_code not like '%688%' group by stock_code) b\n"
					+ "    on a.stock_code=b.stock_code and a.select_date=b.select_date) and 是否涨停='1' and 交易日期= '"
					+ prev + "')  and 交易日期= '" + next + "' ;";

			String testDensenessSql = "select avg(涨跌幅) gain from daily_snapshot where 股票代码 in (\n"
					+ "   select 股票代码 from daily_snapshot where 股票代码 in  \n" + "   (select a.stock_code from \n"
					+ "    (select stock_code, name, select_date, description, open, close, 'downward' as source \n"
					+ "     from denseness where select_date>='" + range.toString() + "' and select_date<= ' " + prev
					+ "' and stock_code not like '%688%') a \n"
					+ "    join (select stock_code, max(select_date) as select_date from densenesswhere stock_code not like '%688%' group by stock_code) b\n"
					+ "    on a.stock_code=b.stock_code and a.select_date=b.select_date) and 是否涨停='1' and 交易日期= '"
					+ prev + "')  and 交易日期= '" + next + "' ;";

			String testSql = "select avg(涨跌幅) gain from daily_snapshot where 股票代码 in (\n"
					+ "   select 股票代码 from daily_snapshot where 股票代码 in  \n" + "   (select a.stock_code from \n"
					+ "    (select stock_code, name, select_date, description, open, close, 'downward' as source \n"
					+ "     from denseness where select_date>='" + range.toString()
					+ "' and stock_code not like '%688%') a \n"
					+ "    join (select stock_code, max(select_date) as select_date from denseness	where stock_code not like '%688%' group by stock_code) b\n"
					+ "    on a.stock_code=b.stock_code and a.select_date=b.select_date) and 是否涨停='1' and 交易日期= '"
					+ prev + "')  and 交易日期= '" + next + "' ;";

			String sql300301 = "select * from (select a.stock_code, a.name, a.select_date, a.description, a.open, a.close, a.source from\n"
					+ " (select stock_code, name, select_date, description, open, close, 'downward' as source from \n"
					+ " downward where select_date>= '" + range + "' and select_date<='" + prev
					+ "' and stock_code not like '%688%' and (stock_code like 'sz300%' or stock_code like 'sz301%') and name not like '%ST%') \n"
					+ " a join "
					+ "	(select stock_code, max(select_date) as select_date from downward where stock_code not like '%688%' and (stock_code like 'sz300%' or stock_code like 'sz301%') and name not like '%ST%'"
					+ "	group by stock_code) b "
					+ "	on a.stock_code=b.stock_code and a.select_date=b.select_date) c " + " join daily_snapshot d on "
					+ " d.股票代码= c.stock_code where d.交易日期='" + next + "';";

			if (interactive) {
				upCappingSql = "select d.股票代码, d.股票名称, 交易日期_prev, 连续涨停_prev, 涨跌幅_prev, 涨跌幅_next, 新浪概念, 新浪行业 from "
						+ " (select 股票代码, 股票名称, 交易日期 交易日期_prev, 连续涨停 连续涨停_prev,涨跌幅 涨跌幅_prev from daily_snapshot where 连续涨停>=1 and 交易日期='"
						+ prev + "') d" + " join"
						+ " (select 股票代码, 交易日期 交易日期_next, 新浪概念, 新浪行业, 涨跌幅 涨跌幅_next, 连续涨停 连续涨停_next from daily_snapshot where 交易日期='"
						+ next + "' and 新浪概念 like '%" + ind + "%'" + ") f on" + " d.股票代码=f.股票代码;";

				downCappingSql = "select d.股票代码, d.股票名称, 交易日期_prev, 连续跌停_prev, 涨跌幅_prev, 涨跌幅_next, 新浪概念, 新浪行业 from "
						+ " (select 股票代码, 股票名称, 交易日期 交易日期_prev, 连续跌停 连续跌停_prev,涨跌幅 涨跌幅_prev from daily_snapshot where 连续跌停>2 and 交易日期='"
						+ prev + "') d" + " join"
						+ " (select 股票代码, 交易日期 交易日期_next, 新浪概念, 新浪行业, 涨跌幅 涨跌幅_next, 连续跌停 连续跌停_next from daily_snapshot where 交易日期='"
						+ next + "' and 新浪概念 like '%" + ind + "%'" + ") f on" + " d.股票代码=f.股票代码;";
			} else {
				upCappingSql = "select d.股票代码, d.股票名称, 交易日期_prev, 连续涨停_prev, 涨跌幅_prev, 涨跌幅_next, 新浪概念, 新浪行业 from "
						+ " (select 股票代码, 股票名称, 交易日期 交易日期_prev, 连续涨停 连续涨停_prev, 涨跌幅 涨跌幅_prev from daily_snapshot where 连续涨停=1 and 交易日期='"
						+ prev + "') d" + " join"
						+ " (select 股票代码, 交易日期 交易日期_next, 新浪概念, 新浪行业, 涨跌幅 涨跌幅_next, 连续涨停 连续涨停_next from daily_snapshot where 交易日期='"
						+ next
						// + "' and 新浪概念 like '%"+ ind +"%'"
						+ "') f on" + " d.股票代码=f.股票代码;";

				downCappingSql = "select d.股票代码, d.股票名称, 交易日期_prev, 连续跌停_prev, 涨跌幅_prev, 涨跌幅_next, 新浪概念, 新浪行业 from "
						+ " (select 股票代码, 股票名称, 交易日期 交易日期_prev, 连续跌停 连续跌停_prev,涨跌幅 涨跌幅_prev from daily_snapshot where 连续跌停>2 and 交易日期='"
						+ prev + "') d" + " join"
						+ " (select 股票代码, 交易日期 交易日期_next, 新浪概念, 新浪行业, 涨跌幅 涨跌幅_next, 连续跌停 连续跌停_next from daily_snapshot where 交易日期='"
						+ next
						// + "' and 新浪概念 like '%"+ ind +"%'"
						+ "') f on" + " d.股票代码=f.股票代码;";
			}

			Statement stmt = ThruBreaker.conn.createStatement();
			ResultSet resultSet = null;

			if (upOrDown.equals("down")) {
				resultSet = stmt.executeQuery(downCappingSql);
			} else if (upOrDown.equals("up")) {
				resultSet = stmt.executeQuery(upCappingSql);
			} else {
				System.out.print("input (up) or (down).");
			}

			ResultSetMetaData rsmd = resultSet.getMetaData();
			int columnsNumber = rsmd.getColumnCount();
			while (resultSet.next()) {
				for (int j = 1; j <= columnsNumber; j++) {
					if (j > 1)
						System.out.print(", ");
					String columnValue = resultSet.getString(j);
					System.out.print(rsmd.getColumnName(j) + ": " + columnValue);
				}
				System.out.println("");
			}
		}
	}

	public static void updateIndustry() {
		String sql = "select * from Listing;";
		new ThruBreaker();
		try {
			Statement stmt = ThruBreaker.conn.createStatement();
			ResultSet resultSet = stmt.executeQuery(sql);
			Set<String> corpSet = new HashSet<String>();
			while (resultSet.next()) {
				corpSet.add(resultSet.getString("stock_code"));
			}
			int cnt = 0;
			PreparedStatement preparedStmt = ThruBreaker.conn.prepareStatement(
					// "update corp_info set industry = ?, lable= ?, organizational_type=? where
					// stock_code = ? ");
					"UPDATE corp_info Set industry=?, lable= ?, organizational_type=? where stock_code = ? "
							+ "If(@@ROWCOUNT=0) "
							+ "INSERT into corp_info (stock_code, industry, lable, organizational_type) Values (?, ?, ?, ?)");

			for (String s : corpSet) {
				// System.out.println(s);
				URL url = new URL("http://api.mairui.club/hscp/gsjj/" + s + "/cd5268626606b8b4ef");
				String jsonStr = IOUtils.toString(url, "UTF-8");
				// jsonStr = jsonStr.replaceAll("\"", "");
				String regex = "\",\"";
				String[] ssss = jsonStr.split(regex);
				// System.out.println("industry updating service(" + LocalDateTime.now() + "): "
				// + s
				// + " Finished industry update HTTP streaming at: " + LocalDateTime.now());

				for (int i = 0; i < ssss.length - 1; i++) {
					ssss[i] = ssss[i].replaceAll("\"", "");
					// ssss[i] = ssss[i].substring(2);
				}
				String industry = null, idea = null, type = null;
				Map<String, String> industryMap = new HashMap<String, String>();
				for (int j = 0; j < ssss.length; j++) {
					if (ssss[j].split(":")[0].equals("bscope")) {
						industry = String.valueOf(ssss[j].split(":")[1]);
						if (industry.length() > 200) {
							industry = industry.substring(0, 200);
						}
					}
					if (ssss[j].split(":")[0].equals("idea")) {
						idea = String.valueOf(ssss[j].split(":")[1]);
						if (idea.length() > 200) {
							idea = idea.substring(0, 200);
						}
					}
					if (ssss[j].split(":")[0].equals("organ")) {
						if (ssss[j].split(":").length > 1) {
							type = String.valueOf(ssss[j].split(":")[1]);
							if (type.length() > 200) {
								type = type.substring(0, 200);
							}
						}
					}
					industryMap.put("industry", industry);
					industryMap.put("idea", idea);
					industryMap.put("type", type);
				}
				// String updateCorpInfo = "update corp_info set industry = '";
				// updateCorpInfo = updateCorpInfo.concat(industryMap.get("industry") + "',
				// lable ='" + industryMap.get("idea")
				// + "', organizational_type ='" + industryMap.get("type") + "' where
				// stock_code='" + s + "';");
				System.out.println("industry service(" + LocalDateTime.now() + "): " + preparedStmt);
				preparedStmt.setString(1, industryMap.get("industry"));
				preparedStmt.setString(2, industryMap.get("idea"));
				preparedStmt.setString(3, industryMap.get("type"));
				preparedStmt.setString(4, s);
				preparedStmt.setString(5, s);
				preparedStmt.setString(6, industryMap.get("industry"));
				preparedStmt.setString(7, industryMap.get("idea"));
				preparedStmt.setString(8, industryMap.get("type"));
				preparedStmt.addBatch();
				cnt++;
				if (cnt % 100 == 0) {
					System.out.println(cnt + " corps, 100 less...");
				}
			}
			preparedStmt.executeBatch();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static Map<Integer, Set<String>> stockSlicing(int NoOfSlices) {
		String stockCode;
		Map<Integer, Set<String>> Slices = new HashMap<Integer, Set<String>>();
		Set<String> slice;
		Integer key = Integer.valueOf(1);
		int NoOfStocks = 0, eachList = 0;
		NoOfStocks = ThruBreaker.allAssetPricesMapping.keySet().size();
		eachList = (NoOfStocks - NoOfStocks % NoOfSlices) / NoOfSlices;
		slice = new HashSet<String>();
		for (String s : ThruBreaker.allAssetPricesMapping.keySet()) {
			stockCode = s;
			slice.add(stockCode);
			if (key <= NoOfSlices) {
				if (slice.size() < eachList) {
				} else {
					Slices.put(key, slice);
					slice = new HashSet<String>();
					key = key + 1;
					// System.out.println("key No. " + key);
				}
			} else if (slice.size() < NoOfStocks % NoOfSlices) {
			} else {
				Slices.put(key, slice);
				break;
			}
		}
		return Slices;
	}

	public static void updateCapped(Statement stmt) throws Exception {
		for (String s : ThruBreaker.allAssetPricesMapping.keySet()) {
			for (AssetStatus as : ThruBreaker.allAssetPricesMapping.get(s).values()) {
				String updateUpCapped = "update daily_snapshot set 连续涨停 = '";
				if (as.upCapped >= 1) {
					updateUpCapped = updateUpCapped
							.concat(as.upCapped + "'" + " where 股票代码 = '" + s + "' and 交易日期 = '" + as.tradeDate + "';");
					System.out.println(updateUpCapped);
					stmt.executeUpdate(updateUpCapped);
				}
				String updateDownCapped = "update daily_snapshot set 连续跌停 = '";
				if (as.downCapped >= 1) {
					updateDownCapped = updateDownCapped.concat(
							as.downCapped + "'" + " where 股票代码 = '" + s + "' and 交易日期 = '" + as.tradeDate + "';");
					System.out.println(updateDownCapped);
					stmt.executeUpdate(updateDownCapped);
				}
			}
		}
	}

	public static void updateCapped(Set<String> codeSet, Statement stmt) throws Exception {
		for (String s : codeSet) {
			for (AssetStatus as : ThruBreaker.allAssetPricesMapping.get(s).values()) {
				String updateUpCapped = "update daily_snapshot set 连续涨停 = '";
				if (as.upCapped >= 1) {
					updateUpCapped = updateUpCapped
							.concat(as.upCapped + "'" + " where 股票代码 = '" + s + "' and 交易日期 = '" + as.tradeDate + "';");
					System.out.println(updateUpCapped);
					stmt.executeUpdate(updateUpCapped);
				}
				String updateDownCapped = "update daily_snapshot set 连续跌停 = '";
				if (as.downCapped >= 1) {
					updateDownCapped = updateDownCapped.concat(
							as.downCapped + "'" + " where 股票代码 = '" + s + "' and 交易日期 = '" + as.tradeDate + "';");
					System.out.println(updateDownCapped);
					stmt.executeUpdate(updateDownCapped);
				}
			}
		}
	}

	public static void updateSparseOriginatedDate(Set<String> codeSet, Date updateAssetInfoSince, Statement stmt)
			throws Exception {
		for (String s : codeSet) {
			for (AssetStatus as : ThruBreaker.allAssetPricesMapping.get(s).values()) {
				if (!as.tradeDate.before(updateAssetInfoSince)) {
					String updateSparse = "update denseness set sparse_originated_on = '";
					if (as.sparseOriginatedDate != null) {
						updateSparse = updateSparse.concat(as.sparseOriginatedDate + "'" + " where stock_code = '" + s
								+ "' and select_date = '" + as.tradeDate + "';");
						System.out.println(updateSparse);
						stmt.executeUpdate(updateSparse);
					}
				}
			}
		}
	}

	public static void updateAssetInfo(Date updateAssetInfoSince) throws Exception {
		LocalDate now = LocalDate.now();
		// new ThruBreaker();
		LocalDate start = now.minusDays(200);
		/*
		 * double quadraticSparseness = 0.025; String SparsenessType = "quadratic";
		 * String SparsenessSpan = "mid";
		 */
		ThruBreaker.setPriceDateRange(start, LocalDate.now());
		Statement stmt = ThruBreaker.conn.createStatement();
		// ThruBreaker.setAllAssetPricesMapping(start, LocalDate.now(),
		// quadraticSparseness, SparsenessType,
		// SparsenessSpan, ThruBreaker.conn, stmt);
		int NoOfSlices = 30;
		Map<Integer, Set<String>> stockSlices = stockSlicing(NoOfSlices);
		Administrator[] Administrators = new Administrator[NoOfSlices + 1];
		LocalDateTime parallelingStart = LocalDateTime.now();
		System.out.println("start parallel processing at: " + parallelingStart);
		String selectmaxUpwardSparseSelectDate = "select max(select_date) maxUpwardSparseSelectDate from denseness "
				+ "where type = 'upward sparse' and sparse_originated_on is not null;";
		ResultSet rs = stmt.executeQuery(selectmaxUpwardSparseSelectDate);
		// truncateQuantTableStatement.execute(truncateQuantTable);
		ThruBreaker.updateUpwardSparseSelectDateSince = null;
		while (rs.next()) {
			ThruBreaker.updateUpwardSparseSelectDateSince = rs.getDate("maxUpwardSparseSelectDate");
		}
		ThruBreaker.updateAssetInfoSince = updateAssetInfoSince;
		for (Integer key : stockSlices.keySet()) {
			Administrators[key - 1] = new Administrator(stockSlices.get(key));
			Administrators[key - 1].start();
			System.out.println(Administrators[key - 1] + ".started();");
		}
		volatilityOfCapped(LocalDate.parse("2024-07-01"), 1);
		/*
		 * for (Integer key : stockSlices.keySet()) { while (Administrators[key -
		 * 1].isAlive() == true) sleep(300); System.out.println(Administrators[key - 1]
		 * + ".isAlive();"); }
		 */
		// updateIndustry();
	}

	public void run() {
		try {
			updateCapped(codeSet, ThruBreaker.conn.createStatement());
			// updateSparseOriginatedDate(codeSet, ThruBreaker.updateAssetInfoSince,
			// ThruBreaker.conn.createStatement());
			System.out.println(this.getName() + " finished@" + LocalDateTime.now());
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void computeCCI(int range, LocalDate priceStart, ThruBreaker tb, Statement stmt) {
		/*
		 * TP = (High + Low + Close) / 3 TPAVG = (TP1 + TP2 +... + TPn) / n MD = (|TP1 -
		 * TPAVG1| +... + | TPn - TPAVGn |) / n CCIt = (TPt - TPAVGt) / (.015 * MDT)
		 */
		// LocalDate priceStart = LocalDate.of(2024, 01, 03);
		ThruBreaker.setPriceDateRange(priceStart, LocalDate.now());
		try {
			ThruBreaker.setAllAssetPricesMappingBasic(priceStart, LocalDate.now(), ThruBreaker.conn, stmt);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		float tp, ma, ma_sum = 0, md = 0, md_sum = 0, cci;

		for (String stockCode : ThruBreaker.allAssetPricesMapping.keySet()) {
			for (LocalDate d : ThruBreaker.priceDateRange) {
				ma_sum = md_sum = 0;
				AssetStatus as = ThruBreaker.allAssetPricesMapping.get(stockCode)
						.get(java.util.Date.from(d.atStartOfDay(defaultZoneId).toInstant()));
				if (as != null) {
					as.typicalPrice = (as.highPrice + as.lowPrice + as.closePrice) / 3;
					if (ThruBreaker.priceDateRange.indexOf(d) - range >= 0) {
						for (LocalDate m : ThruBreaker.priceDateRange.subList(tb.priceDateRange.indexOf(d) - range + 1,
								ThruBreaker.priceDateRange.indexOf(d) + 1)) {
							if (ThruBreaker.allAssetPricesMapping.get(stockCode)
									.get(java.util.Date.from(m.atStartOfDay(defaultZoneId).toInstant())) != null)
								ma_sum += ThruBreaker.allAssetPricesMapping.get(stockCode).get(
										java.util.Date.from(m.atStartOfDay(defaultZoneId).toInstant())).typicalPrice;
						}
						ma = ma_sum / range;
						as.cciMA = ma;
						for (LocalDate m : ThruBreaker.priceDateRange.subList(tb.priceDateRange.indexOf(d) - range + 1,
								ThruBreaker.priceDateRange.indexOf(d) + 1)) {
							if (ThruBreaker.allAssetPricesMapping.get(stockCode)
									.get(java.util.Date.from(m.atStartOfDay(defaultZoneId).toInstant())) != null)
								md_sum += Math.abs(ThruBreaker.allAssetPricesMapping.get(stockCode).get(
										java.util.Date.from(m.atStartOfDay(defaultZoneId).toInstant())).typicalPrice
										- ThruBreaker.allAssetPricesMapping.get(stockCode).get(
												java.util.Date.from(m.atStartOfDay(defaultZoneId).toInstant())).cciMA);
						}
						md = md_sum / range;
						as.cciMD = md;
						cci = (float) ((as.typicalPrice - as.cciMA) / (as.cciMD * 0.015));
						as.cci = cci;
					}
				}
			}
		}
		return;
	}

	class cciOnDate {
		java.util.Date date;
		double cci;
		double cciHat;
		public float closePrice;

		cciOnDate(java.util.Date date, double cci) {
			this.date = date;
			this.cci = cci;
		}
	}

	class DescendingCCIOrder implements Comparator<cciOnDate> {
		public int compare(cciOnDate o1, cciOnDate o2) {
			double i1 = o1.cci;
			double i2 = o2.cci;
			return i1 < i2 ? 1 : -1;
		}
	}

	class DescendingCloseOrder implements Comparator<cciOnDate> {
		public int compare(cciOnDate o1, cciOnDate o2) {
			double i1 = o1.closePrice;
			double i2 = o2.closePrice;
			return i1 < i2 ? 1 : -1;
		}
	}

	class AscendingDateOrder implements Comparator<cciOnDate> {
		public int compare(cciOnDate o1, cciOnDate o2) {
			Date i1 = (Date) o1.date;
			Date i2 = (Date) o2.date;
			return i1.compareTo(i2);
		}
	}

	public static void cciExpolaration(int scale, LocalDate startDate, java.util.Date expolarationDate) {
		ThruBreaker tb = new ThruBreaker();
		Statement stmt = null;
		try {
			stmt = ThruBreaker.conn.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Administrator admin = new Administrator();
		computeCCI(scale, startDate, tb, stmt);

		List<cciOnDate> cciList;
		Map<String, List<cciOnDate>> cciMap = new HashMap<String, List<cciOnDate>>();
		int index1 = 0, index2 = 0, maxDateIndex = 0;
		cciOnDate CapeCod;
		double maxCci = 0;
		java.util.Date maxDate = null;
		double slope, maxSlope = 0;
		DescendingCCIOrder dco = admin.new DescendingCCIOrder();
		for (String code : ThruBreaker.allAssetPricesMapping.keySet()) {
			cciList = new ArrayList<cciOnDate>();
			for (java.util.Date d : ThruBreaker.allAssetPricesMapping.get(code).keySet()) {
				if (ThruBreaker.allAssetPricesMapping.get(code).get(d).cci != 0.0 && !d.after(expolarationDate)) {
					cciOnDate cid = admin.new cciOnDate(d, ThruBreaker.allAssetPricesMapping.get(code).get(d).cci);
					cciList.add(cid);
				}
			}
			Collections.sort(cciList, dco);
			if (!cciList.isEmpty()) {
				CapeCod = cciList.get(0);
				maxCci = CapeCod.cci;
				maxDate = CapeCod.date;
				maxDateIndex = ThruBreaker.priceDateRange
						.indexOf(Instant.ofEpochMilli(maxDate.getTime()).atZone(ZoneId.systemDefault()).toLocalDate());
				maxSlope = 0;
				for (cciOnDate cod : cciList) {
					if (cod.date.after(maxDate)) {
						index1 = ThruBreaker.priceDateRange.indexOf(
								Instant.ofEpochMilli(cod.date.getTime()).atZone(ZoneId.systemDefault()).toLocalDate());
						slope = (cod.cci - maxCci) / (index1 - maxDateIndex);
						if (maxSlope == 0) {
							maxSlope = slope;
						}
						if (slope > maxSlope) {
							maxSlope = slope;
						}
					}
				}
			}
			cciList = new ArrayList<cciOnDate>();
			for (java.util.Date d : ThruBreaker.allAssetPricesMapping.get(code).keySet()) {
				if (d != null && maxDate != null && d.after(maxDate)) {
					index2 = ThruBreaker.priceDateRange
							.indexOf(Instant.ofEpochMilli(d.getTime()).atZone(ZoneId.systemDefault()).toLocalDate());
					cciOnDate cod = admin.new cciOnDate(d, ThruBreaker.allAssetPricesMapping.get(code).get(d).cci);
					cod.cciHat = maxCci + (index2 - maxDateIndex) * maxSlope;
					cciList.add(cod);
					System.out.println("date: " + cod.date + ", hat: " + cod.cciHat);
				}
			}
			cciMap.put(code, cciList);
		}

		Map<String, List<cciOnDate>> cciUpMap = new HashMap<String, List<cciOnDate>>();
		List<cciOnDate> upList;
		for (String code : ThruBreaker.allAssetPricesMapping.keySet()) {
			List<cciOnDate> sortedDateList = cciMap.get(code);
			Collections.sort(sortedDateList, admin.new AscendingDateOrder());
			ListIterator<cciOnDate> li = sortedDateList.listIterator();

			cciOnDate p = null, n = null;
			upList = new ArrayList<cciOnDate>();
			while (li.hasNext()) {
				if (li.hasPrevious()) {
					p = li.previous();
				}
				li.next();
				if (li.hasNext()) {
					n = li.next();
				}
				if (li.hasNext() && li.hasPrevious())
					System.out.println(n.date + ", CCI: " + n.cci + ", CCIHat: " + n.cciHat);
				if (p != null && n != null && p.cci < p.cciHat && n.cci > n.cciHat
						&& ThruBreaker.allAssetPricesMapping.get(code).get(n.date).changeRate >= 0.05) {
					System.out.println("UP!");
					upList.add(n);
					cciUpMap.put(code, upList);
				}
			}
		}
		float closePrice, closePriceOnCciUpDate;
		List<cciOnDate> closePrices;
		String cciSql;
		PreparedStatement preparedStmt;
		try {
			String truncateCciResults = "truncate cci_results;";
			stmt = ThruBreaker.conn.createStatement();
			stmt.executeQuery(truncateCciResults);
			preparedStmt = ThruBreaker.conn.prepareStatement(
					"insert into cci_results (stock_code, stock_name, trade_date, cci_scale, cci, cci_hat, max_day, gain, description) values (?, ?, ?, ?, ?, ?, ?, ?, ?)");

			for (String s : cciUpMap.keySet()) {
				closePrices = new ArrayList<cciOnDate>();
				closePriceOnCciUpDate = 0;
				for (cciOnDate cci : cciUpMap.get(s)) {
					for (java.util.Date d : ThruBreaker.allAssetPricesMapping.get(s).keySet()) {
						if (d.equals(cci.date)) {
							closePriceOnCciUpDate = ThruBreaker.allAssetPricesMapping.get(s).get(d).closePrice;
						}
						if (d.after(cci.date)) {
							closePrice = ThruBreaker.allAssetPricesMapping.get(s).get(d).closePrice;
							cciOnDate price = admin.new cciOnDate(cci.date, cci.cci);
							price.closePrice = closePrice;
							price.date = d;
							closePrices.add(price);
						}
					}
					System.out.print(s + ", " + cci.date + ", cci: " + cci.cci + ", hat: " + cci.cciHat);
					if (!closePrices.isEmpty()) {
						Collections.sort(closePrices, admin.new DescendingCloseOrder());
						System.out.println(", with max: " + closePrices.get(0).closePrice + " and gain, "
								+ String.format("%.2f",
										(closePrices.get(0).closePrice / closePriceOnCciUpDate - 1) * 100)
								+ "%" + ", on " + closePrices.get(0).date);
						preparedStmt.setString(1, s);
						preparedStmt.setString(2, ThruBreaker.allAssetPricesMapping.get(s).get(cci.date).stockName);
						preparedStmt.setDate(3, (Date) cci.date);
						preparedStmt.setInt(4, scale);
						preparedStmt.setDouble(5, cci.cci);
						preparedStmt.setDouble(6, cci.cciHat);
						preparedStmt.setDate(7, (Date) closePrices.get(0).date);
						preparedStmt.setDouble(8, (closePrices.get(0).closePrice / closePriceOnCciUpDate - 1) * 100);
						preparedStmt.setString(9, (closePrices.isEmpty())
								? s + ", " + cci.date + ", cci scale: " + scale + ", cci: " + cci.cci + ", hat: "
										+ cci.cciHat
								: s + ", " + cci.date + ", cci: " + cci.cci + ", hat: " + cci.cciHat + ", with max: "
										+ closePrices.get(0).closePrice + " and gain, "
										+ String.format("%.2f",
												(closePrices.get(0).closePrice / closePriceOnCciUpDate - 1) * 100)
										+ "%" + ", on " + closePrices.get(0).date);
						preparedStmt.execute();
					} else {
						System.out.println();
						preparedStmt.setString(1, s);
						preparedStmt.setString(2, ThruBreaker.allAssetPricesMapping.get(s).get(cci.date).stockName);
						preparedStmt.setDate(3, (Date) cci.date);
						preparedStmt.setInt(4, scale);
						preparedStmt.setDouble(5, cci.cci);
						preparedStmt.setDouble(6, cci.cciHat);
						preparedStmt.execute();
					}
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void volatilityTrading(LocalDate startld, Date updateVolatilitySince) {
		int index;
		Asset utilityAsset = new Asset();
		double[] closePrices;
		int i;
		double volatility120 = 0, volatility220 = 0, volatility90 = 0, volatility60 = 0, volatility30 = 0,
				volatility10 = 0, volatility5 = 0, volatility3 = 0, volatility120_standardized = 0,
				volatility220_standardized = 0, volatility90_standardized = 0, volatility60_standardized = 0,
				volatility30_standardized = 0, volatility10_standardized = 0, volatility5_standardized = 0,
				volatility3_standardized = 0;
		new ThruBreaker();
		// String truncateVolatility = "truncate volatility;";
		String maxDateInVolatility = "select max(trade_date) max_date from volatility";
		try {
			Statement stmt = ThruBreaker.conn.createStatement();
			ThruBreaker.setPriceDateRange(startld, LocalDate.now());
			ThruBreaker.setAllAssetPricesMappingBasic(startld, LocalDate.now(), ThruBreaker.conn, stmt);
			// stmt.executeQuery(truncateVolatility);
			ResultSet rs = stmt.executeQuery(maxDateInVolatility);
			Date maxDate = null;
			while (rs.next()) {
				maxDate = rs.getDate("max_date");
			}

			String deleteVolatility = "delete from volatility where trade_date >= '" + updateVolatilitySince.toString()
					+ "';";
			System.out.println(deleteVolatility);
			stmt.executeQuery(deleteVolatility);
			PreparedStatement preparedStmt = ThruBreaker.conn.prepareStatement(
					"insert into volatility (stock_code, stock_name, trade_date, volatility_220, volatility_120, volatility_90, volatility_60, "
							+ "volatility_30, volatility_10, volatility_5, volatility_3, volatility_220_standardized, volatility_120_standardized, volatility_90_standardized, "
							+ "volatility_60_standardized, volatility_30_standardized, volatility_10_standardized, volatility_5_standardized, volatility_3_standardized) "
							+ "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
			for (String code : ThruBreaker.allAssetPricesMapping.keySet()) {
				for (java.util.Date d : ThruBreaker.allAssetPricesMapping.get(code).keySet()) {
					if (!d.before(updateVolatilitySince)) {
						index = ThruBreaker.priceDateRange.indexOf(
								Instant.ofEpochMilli(d.getTime()).atZone(ZoneId.systemDefault()).toLocalDate());
						i = 0;
						closePrices = new double[index];
						while (i < index) {
							if (ThruBreaker.allAssetPricesMapping.get(code)
									.get(java.util.Date.from(ThruBreaker.priceDateRange.get(i)
											.atStartOfDay(defaultZoneId).toInstant())) != null) {
								closePrices[i] = ThruBreaker.allAssetPricesMapping.get(code)
										.get(java.util.Date.from(ThruBreaker.priceDateRange.get(i)
												.atStartOfDay(defaultZoneId).toInstant())).closePrice;
							}
							i++;
						}
						if (index > 220) {
							volatility220 = utilityAsset.numericVolatility(
									Arrays.copyOfRange(closePrices, closePrices.length - 221, closePrices.length - 1));
							ThruBreaker.allAssetPricesMapping.get(code).get(d).volatility220 = volatility220;
						}
						if (index > 120) {
							volatility120 = utilityAsset.numericVolatility(
									Arrays.copyOfRange(closePrices, closePrices.length - 121, closePrices.length - 1));
							ThruBreaker.allAssetPricesMapping.get(code).get(d).volatility120 = volatility120;
						}
						if (index > 90) {
							volatility90 = utilityAsset.numericVolatility(
									Arrays.copyOfRange(closePrices, closePrices.length - 91, closePrices.length - 1));
							ThruBreaker.allAssetPricesMapping.get(code).get(d).volatility90 = volatility90;
						}
						if (index > 60) {
							volatility60 = utilityAsset.numericVolatility(
									Arrays.copyOfRange(closePrices, closePrices.length - 61, closePrices.length - 1));
							ThruBreaker.allAssetPricesMapping.get(code).get(d).volatility60 = volatility60;
						}
						if (index > 30) {
							volatility30 = utilityAsset.numericVolatility(
									Arrays.copyOfRange(closePrices, closePrices.length - 31, closePrices.length - 1));
							ThruBreaker.allAssetPricesMapping.get(code).get(d).volatility30 = volatility30;
						}
						if (index > 10) {
							volatility10 = utilityAsset.numericVolatility(
									Arrays.copyOfRange(closePrices, closePrices.length - 11, closePrices.length - 1));
							ThruBreaker.allAssetPricesMapping.get(code).get(d).volatility10 = volatility10;
						}
						if (index > 5) {
							volatility5 = utilityAsset.numericVolatility(
									Arrays.copyOfRange(closePrices, closePrices.length - 6, closePrices.length - 1));
							ThruBreaker.allAssetPricesMapping.get(code).get(d).volatility5 = volatility5;
						}
						if (index > 3) {
							volatility3 = utilityAsset.numericVolatility(
									Arrays.copyOfRange(closePrices, closePrices.length - 4, closePrices.length - 1));
							ThruBreaker.allAssetPricesMapping.get(code).get(d).volatility3 = volatility3;
						}

						if (index > 220) {
							volatility220_standardized = utilityAsset.standardizedVolatility(
									Arrays.copyOfRange(closePrices, closePrices.length - 221, closePrices.length - 1));
							ThruBreaker.allAssetPricesMapping.get(code).get(d).volatility220Standardized = volatility220_standardized;
						}
						if (index > 120) {
							volatility120_standardized = utilityAsset.standardizedVolatility(
									Arrays.copyOfRange(closePrices, closePrices.length - 121, closePrices.length - 1));
							ThruBreaker.allAssetPricesMapping.get(code).get(d).volatility120Standardized = volatility120_standardized;
						}
						if (index > 90) {
							volatility90_standardized = utilityAsset.standardizedVolatility(
									Arrays.copyOfRange(closePrices, closePrices.length - 91, closePrices.length - 1));
							ThruBreaker.allAssetPricesMapping.get(code).get(d).volatility90Standardized = volatility90_standardized;
						}
						if (index > 60) {
							volatility60_standardized = utilityAsset.standardizedVolatility(
									Arrays.copyOfRange(closePrices, closePrices.length - 61, closePrices.length - 1));
							ThruBreaker.allAssetPricesMapping.get(code).get(d).volatility60Standardized = volatility60_standardized;
						}
						if (index > 30) {
							volatility30_standardized = utilityAsset.standardizedVolatility(
									Arrays.copyOfRange(closePrices, closePrices.length - 31, closePrices.length - 1));
							ThruBreaker.allAssetPricesMapping.get(code).get(d).volatility30Standardized = volatility30_standardized;
						}
						if (index > 10) {
							volatility10_standardized = utilityAsset.standardizedVolatility(
									Arrays.copyOfRange(closePrices, closePrices.length - 11, closePrices.length - 1));
							ThruBreaker.allAssetPricesMapping.get(code).get(d).volatility10Standardized = volatility10_standardized;
						}
						if (index > 5) {
							volatility5_standardized = utilityAsset.standardizedVolatility(
									Arrays.copyOfRange(closePrices, closePrices.length - 6, closePrices.length - 1));
							ThruBreaker.allAssetPricesMapping.get(code).get(d).volatility5Standardized = volatility5_standardized;
						}
						if (index > 3) {
							volatility3_standardized = utilityAsset.standardizedVolatility(
									Arrays.copyOfRange(closePrices, closePrices.length - 4, closePrices.length - 1));
							ThruBreaker.allAssetPricesMapping.get(code).get(d).volatility3Standardized = volatility3_standardized;
						}

						preparedStmt.setString(1, code);
						preparedStmt.setString(2, ThruBreaker.allAssetPricesMapping.get(code).get(d).stockName);
						preparedStmt.setDate(3, new java.sql.Date(d.getTime()));
						preparedStmt.setDouble(4, volatility220);
						preparedStmt.setDouble(5, volatility120);
						preparedStmt.setDouble(6, volatility90);
						preparedStmt.setDouble(7, volatility60);
						preparedStmt.setDouble(8, volatility30);
						preparedStmt.setDouble(9, volatility10);
						preparedStmt.setDouble(10, volatility5);
						preparedStmt.setDouble(11, volatility3);
						preparedStmt.setDouble(12, volatility220_standardized);
						preparedStmt.setDouble(13, volatility120_standardized);
						preparedStmt.setDouble(14, volatility90_standardized);
						preparedStmt.setDouble(15, volatility60_standardized);
						preparedStmt.setDouble(16, volatility30_standardized);
						preparedStmt.setDouble(17, volatility10_standardized);
						preparedStmt.setDouble(18, volatility5_standardized);
						preparedStmt.setDouble(19, volatility3_standardized);
						System.out.println(preparedStmt.toString());
						preparedStmt.execute();
					}
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void volatilityOfCapped(LocalDate startld, int threshold) {
		int index, indexBeforeInitiation;
		double[] closePrices;
		int i;
		double volatility120 = 0, volatility220 = 0, volatility90 = 0, volatility60 = 0, volatility30 = 0,
				volatility10 = 0, volatility5 = 0, volatility3 = 0, volatility120_standardized = 0,
				volatility220_standardized = 0, volatility90_standardized = 0, volatility60_standardized = 0,
				volatility30_standardized = 0, volatility10_standardized = 0, volatility5_standardized = 0,
				volatility3_standardized = 0;
		new ThruBreaker();
		String truncateVolatilityStat = "truncate volatility_stat;";
		Statement stmt;
		PreparedStatement preparedStmt, preparedStmt2;
		try {
			stmt = ThruBreaker.conn.createStatement();
			stmt.executeQuery(truncateVolatilityStat);
			preparedStmt = ThruBreaker.conn
					.prepareStatement("select * from volatility where stock_code = ? and trade_date = ?");
			preparedStmt2 = ThruBreaker.conn
					.prepareStatement("insert into volatility_stat (stock_code, stock_name, trade_date, "
							+ "initiation_date, capped, volatility_120_standardized, volatility_90_standardized, "
							+ "volatility_60_standardized, volatility_30_standardized, volatility_10_standardized, long_backward_sparsness_denseness, price_trend) "
							+ "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
			ThruBreaker.setPriceDateRange(startld, LocalDate.now());
			LocalDate priceTill = LocalDate.now();

			double quadraticSparseness = 0.025;
			String SparsenessType = "quadratic";
			String SparsenessSpan = "mid";
			// ThruBreaker.setAllAssetPricesMapping(startld, priceTill, quadraticSparseness,
			// SparsenessType, SparsenessSpan, ThruBreaker.conn, stmt);
			ResultSet rs;

			for (String code : ThruBreaker.allAssetPricesMapping.keySet()) {
				for (java.util.Date d : ThruBreaker.allAssetPricesMapping.get(code).keySet()) {
					System.out.println(
							code + ", " + d + ": " + ThruBreaker.allAssetPricesMapping.get(code).get(d).upCapped);
					if (ThruBreaker.allAssetPricesMapping.get(code).get(d).upCapped >= threshold) {
						indexBeforeInitiation = ThruBreaker.priceDateRange
								.indexOf(Instant.ofEpochMilli(d.getTime()).atZone(ZoneId.systemDefault()).toLocalDate())
								- ThruBreaker.allAssetPricesMapping.get(code).get(d).upCapped;
						if (indexBeforeInitiation < 0) {
							continue;
						}
						preparedStmt.setString(1, code);
						preparedStmt.setDate(2,
								java.sql.Date.valueOf(ThruBreaker.priceDateRange.get(indexBeforeInitiation)));
						rs = preparedStmt.executeQuery();
						while (rs.next()) {
							volatility10_standardized = rs.getDouble("volatility_10_standardized");
							volatility30_standardized = rs.getDouble("volatility_30_standardized");
							volatility60_standardized = rs.getDouble("volatility_60_standardized");
							volatility90_standardized = rs.getDouble("volatility_90_standardized");
							volatility120_standardized = rs.getDouble("volatility_120_standardized");
						}
						System.out.println(code + "(" + ThruBreaker.allAssetPricesMapping.get(code).get(d).stockName
								+ ") reached " + ThruBreaker.allAssetPricesMapping.get(code).get(d).upCapped
								+ " upCapped. " + "@" + d + ", with iniation day: "
								+ ThruBreaker.priceDateRange.get(indexBeforeInitiation)
								+ ", with volatility60_standardized: " + volatility60_standardized
								+ ", volatility90_standardized: " + volatility90_standardized
								+ " volatility30_standardized: " + volatility30_standardized);
						preparedStmt2.setString(1, code);
						preparedStmt2.setString(2, ThruBreaker.allAssetPricesMapping.get(code).get(d).stockName);
						preparedStmt2.setDate(3, java.sql.Date.valueOf(d.toString()));
						preparedStmt2.setDate(4, java.sql.Date
								.valueOf(ThruBreaker.priceDateRange.get(indexBeforeInitiation).toString()));
						preparedStmt2.setInt(5, ThruBreaker.allAssetPricesMapping.get(code).get(d).upCapped);
						preparedStmt2.setDouble(6, volatility120_standardized);
						preparedStmt2.setDouble(7, volatility90_standardized);
						preparedStmt2.setDouble(8, volatility60_standardized);
						preparedStmt2.setDouble(9, volatility30_standardized);
						preparedStmt2.setDouble(10, volatility10_standardized);
						if (indexBeforeInitiation > 0) {
							preparedStmt2.setDouble(11,
									ThruBreaker.allAssetPricesMapping.get(code)
											.get(ThruBreaker.accessPriceDateRange(
													ThruBreaker.priceDateRange.get(indexBeforeInitiation),
													0)).sparsenessSuperLong);
							preparedStmt2.setDouble(12,
									ThruBreaker.allAssetPricesMapping.get(code)
											.get(ThruBreaker.accessPriceDateRange(
													ThruBreaker.priceDateRange.get(indexBeforeInitiation),
													0)).closePriceTrend60);
						}
						preparedStmt2.execute();
					}
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		// morningBuyingTest(interactive, "up", now.minusDays(25));
		// cciExpolaration(88, LocalDate.of(2024, 6, 03),
		// java.util.Date.from(LocalDate.of(2025, 2,
		// 28).atStartOfDay(defaultZoneId).toInstant()));
		LocalDateTime startDT = LocalDateTime.now();
		volatilityTrading(LocalDate.parse("2024-01-01"), java.sql.Date.valueOf("2024-10-01"));
		// Date.valueOf(LocalDate.parse("2025-01-01")));
		//updateAssetInfo(Date.valueOf(LocalDate.parse("2024-12-14")));
		//volatilityOfCapped(LocalDate.parse("2025-01-01"), 1);
		LocalDateTime endDT = LocalDateTime.now();
		System.out.println("compuation end time: " + endDT);
		System.out.println("compuation duration: " + startDT.until(endDT, ChronoUnit.SECONDS) + " seconds.");

	}
}
