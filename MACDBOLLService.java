package StockMarket;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class MACDBOLLService extends Thread {

	static LocalDateTime startDT, endDT;
	static int runControlID = 0;

	public void MACDBOLLBreakThru() {
		String macdBollSql = "insert into macd_boll (stock_code, stock_name, up_date, up_times, macd_status, boll_up, description, intraday) values (?, ?, ?, ?, ?, ?, ?, ? )";
		String macdBollSql_intraday = "insert into macd_boll_intraday (stock_code, stock_name, up_date, up_times, macd_status, boll_up, description) values (?, ?, ?, ?, ?, ?, ?)";
		String truncateSql = "truncate macd_boll;";
		String delete_intraday_Sql = "delete from macd_boll_intraday where up_date = ?;";
		while (true) {
			// if (LocalDateTime.now().isAfter(startDT.plusMinutes(1).plusSeconds(1))) {
			if (runControlID > 1) {
				try {
					Thread.sleep(60000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			runControlID++;
			startDT = LocalDateTime.now();
			System.out
					.println("MACDBOLL service(" + LocalDateTime.now() + "): Starting MACDBOLL gauging at: " + startDT);
			Double deviation;

			ResultSet rs;
			Statement stmt;
			PreparedStatement preparedStmt, preparedStmt_intraday, preparedStmt_delete_intraday;

			LocalDate priceStart = LocalDate.of(2024, 12, 30);
			String macdStatus = "";
			String descriptionCode, descriptionName;
			int nextDaysIter;
			double siganlPrice;
			Map<String, String> macd_boll = new HashMap<String, String>();
			AssetStatus todayAssetStatus, yesterdayAssetStatus;
			java.util.Date intraday = java.util.Date
					.from(LocalDate.now().atStartOfDay(ZoneId.systemDefault()).toInstant());
			java.sql.Date intradaySql = java.sql.Date.valueOf(LocalDate.now());
			try {
				stmt = ThruBreaker.conn.createStatement();
				ThruBreaker.setPriceDateRange(priceStart, LocalDate.now());
				ThruBreaker.setAllAssetPricesMappingBasic(priceStart, LocalDate.now(), ThruBreaker.conn, stmt);
				stmt.execute(truncateSql);
				preparedStmt = ThruBreaker.conn.prepareStatement(macdBollSql);
				preparedStmt_intraday = ThruBreaker.conn.prepareStatement(macdBollSql_intraday);
				preparedStmt_delete_intraday = ThruBreaker.conn.prepareStatement(delete_intraday_Sql);
				preparedStmt_delete_intraday.setDate(1, intradaySql);
				preparedStmt_delete_intraday.execute();
				String nextClosePrices = null;
				for (LocalDate everyday : ThruBreaker.priceDateRange) {
					for (String key : ThruBreaker.allAssetPricesMapping.keySet()) {
						// if(key.equals("sz300221") && everyday.equals(LocalDate.of(2025,04,02))) {
						// System.out.println("银禧科技");
						todayAssetStatus = ThruBreaker.allAssetPricesMapping.get(key)
								.get(ThruBreaker.accessPriceDateRange(everyday, 0));
						yesterdayAssetStatus = ThruBreaker.allAssetPricesMapping.get(key)
								.get(ThruBreaker.accessPriceDateRange(everyday, -1));
						// if (key.contains("sz300") || key.contains("sz301")) {
						if (todayAssetStatus != null) {
							if (todayAssetStatus.MACDGoldCross != null && !todayAssetStatus.MACDGoldCross.isEmpty()) {
								if (todayAssetStatus.macd_macd != 0 && todayAssetStatus.macd_macd > 0) {
									todayAssetStatus.macdGoldCrossStatus = "水上".concat(todayAssetStatus.MACDGoldCross);
								} else if (todayAssetStatus.macd_macd != 0 && todayAssetStatus.macd_macd < 0) {
									todayAssetStatus.macdGoldCrossStatus = "水下".concat(todayAssetStatus.MACDGoldCross);
								}
								todayAssetStatus.closeAboveBollUpHappening = 0;
							} else {
								if (yesterdayAssetStatus != null)
									todayAssetStatus.macdGoldCrossStatus = yesterdayAssetStatus.macdGoldCrossStatus;
							}

							try {
								if (todayAssetStatus != null && todayAssetStatus.bollUp <= todayAssetStatus.closePrice
										&& (!todayAssetStatus.tradeDate.equals(intraday)
												&& todayAssetStatus.macdGoldCrossStatus.contains("金叉")
												|| (todayAssetStatus.tradeDate.equals(intraday)))
										 && yesterdayAssetStatus != null
										// && (yesterdayAssetStatus != null
										// && yesterdayAssetStatus.bollUp >= yesterdayAssetStatus.closePrice)
										&& ((todayAssetStatus.changeRate >= 0.05
												&& (key.contains("sz300") || key.contains("sz301"))
												|| (todayAssetStatus.changeRate >= 0.045
														&& !(key.contains("sz300") || key.contains("sz301")))))) {
									System.out.println(key.trim() + ", on " + todayAssetStatus.tradeDate
											+ ", macdGoldCrossStatus: " + todayAssetStatus.macdGoldCrossStatus
											+ ", change rate: " + todayAssetStatus.changeRate);
									todayAssetStatus.closeAboveBollUpHappening = yesterdayAssetStatus.closeAboveBollUpHappening
											+ 1;
									siganlPrice = todayAssetStatus.closePrice;
									// if (todayAssetStatus.closeAboveBollUpHappening >= 1) {

									descriptionName = todayAssetStatus.stockName + " on " + everyday + " "
											+ todayAssetStatus.macdGoldCrossStatus + ", bollUp times: "
											+ todayAssetStatus.closeAboveBollUpHappening + " priced at: "
											+ todayAssetStatus.closePrice + ", change rate: "
											+ String.format("%.2f", todayAssetStatus.changeRate * 100) + "%, signal...";
									descriptionCode = todayAssetStatus.stockCode + " on " + everyday + " "
											+ todayAssetStatus.macdGoldCrossStatus + ", bollUp times: "
											+ todayAssetStatus.closeAboveBollUpHappening + " priced at: "
											+ todayAssetStatus.closePrice + ", change rate: "
											+ String.format("%.2f", todayAssetStatus.changeRate * 100) + "%, signal...";

									System.out.println(
											"MACDBOLL service(" + LocalDateTime.now() + "): " + descriptionName);
									nextClosePrices = new String();
									System.out.print(key + " on " + everyday + " next close prices: ");
									nextClosePrices = nextClosePrices
											.concat(key + " on " + everyday + " next close prices: ");
									for (nextDaysIter = 0; nextDaysIter < 20; nextDaysIter++) {
										try {
											if (ThruBreaker.allAssetPricesMapping.get(key).get(
													ThruBreaker.accessPriceDateRange(everyday, nextDaysIter)) != null) {
												System.out.print(String.format("%.2f",
														(ThruBreaker.allAssetPricesMapping.get(key)
																.get(ThruBreaker.accessPriceDateRange(everyday,
																		nextDaysIter)).closePrice
																/ siganlPrice - 1) * 100)
														+ "%, ");
												nextClosePrices = nextClosePrices.concat(String.format("%.2f",
														(ThruBreaker.allAssetPricesMapping.get(key)
																.get(ThruBreaker.accessPriceDateRange(everyday,
																		nextDaysIter)).closePrice
																/ siganlPrice - 1) * 100)
														+ "%, ");
											}
										} catch (java.lang.IndexOutOfBoundsException iobx) {
											iobx.printStackTrace();
										}
									}
									System.out.print("\n ");

									if (everyday.equals(LocalDate.of(2025, 04, 07))) {
										System.out.println("tradeDate.equals(intraday): " + todayAssetStatus.tradeDate);
									}

									if (todayAssetStatus.tradeDate.equals(new java.util.Date(intraday.getTime()))) {
										System.out.println("tradeDate.equals(intraday): " + todayAssetStatus.tradeDate);
										macd_boll.put(todayAssetStatus.stockCode, "MACDBOLL intraday service("
												+ LocalDateTime.now() + "): " + descriptionCode);
									}
									if (todayAssetStatus.tradeDate.equals(intraday)) {
										preparedStmt_intraday.setString(1, todayAssetStatus.stockCode);
										preparedStmt_intraday.setString(2, todayAssetStatus.stockName);
										preparedStmt_intraday.setDate(3,
												new java.sql.Date(todayAssetStatus.tradeDate.getTime()));
										preparedStmt_intraday.setInt(4, todayAssetStatus.closeAboveBollUpHappening);
										preparedStmt_intraday.setString(5, todayAssetStatus.macdGoldCrossStatus);
										preparedStmt_intraday.setDouble(6, todayAssetStatus.bollUp);
										preparedStmt_intraday.setString(7, descriptionCode + nextClosePrices);
										preparedStmt_intraday.execute();
										preparedStmt.setString(1, todayAssetStatus.stockCode);
										preparedStmt.setString(2, todayAssetStatus.stockName);
										preparedStmt.setDate(3,
												new java.sql.Date(todayAssetStatus.tradeDate.getTime()));
										preparedStmt.setInt(4, todayAssetStatus.closeAboveBollUpHappening);
										preparedStmt.setString(5, todayAssetStatus.macdGoldCrossStatus);
										preparedStmt.setDouble(6, todayAssetStatus.bollUp);
										preparedStmt.setString(7, descriptionCode + nextClosePrices);
										preparedStmt.setBoolean(8, true);
										preparedStmt.execute();
									} else {
										preparedStmt.setString(1, todayAssetStatus.stockCode);
										preparedStmt.setString(2, todayAssetStatus.stockName);
										preparedStmt.setDate(3,
												new java.sql.Date(todayAssetStatus.tradeDate.getTime()));
										preparedStmt.setInt(4, todayAssetStatus.closeAboveBollUpHappening);
										preparedStmt.setString(5, todayAssetStatus.macdGoldCrossStatus);
										preparedStmt.setDouble(6, todayAssetStatus.bollUp);
										preparedStmt.setString(7, descriptionName + nextClosePrices);
										preparedStmt.setBoolean(8, false);
										preparedStmt.execute();
									}
								} else {
									if(yesterdayAssetStatus != null && todayAssetStatus!=null)
									todayAssetStatus.closeAboveBollUpHappening = yesterdayAssetStatus.closeAboveBollUpHappening;
								}
							} catch (java.lang.NullPointerException npx) {
								npx.printStackTrace();
								//continue;
							} 
							 catch (java.lang.Exception x) {
									x.printStackTrace();
									//continue;
								} 
						}
					}
				}
//					if (key.equals("sz300871") && everyday.equals(LocalDate.of(2025, 03, 25))) {
//
//						System.out.println(ThruBreaker.allAssetPricesMapping.get(key)
//								.get(ThruBreaker.accessPriceDateRange(everyday, 0)).macdGoldCrossStatus);
//
//						if (ThruBreaker.allAssetPricesMapping.get(key)
//								.get(ThruBreaker.accessPriceDateRange(everyday, 0)).macdGoldCrossStatus == null) {
//							System.out.println("null");
//						}
//						if (ThruBreaker.allAssetPricesMapping.get(key)
//								.get(ThruBreaker.accessPriceDateRange(everyday, 0)).macdGoldCrossStatus.isEmpty()) {
//							System.out.println("empty");
//						} else {
//							System.out.println("neither");
//						}
//
//						System.out.println(ThruBreaker.allAssetPricesMapping.get(key)
//								.get(ThruBreaker.accessPriceDateRange(everyday, 0)).stockName + " on " + everyday
//								+ ", goldcross status: "
//								+ ThruBreaker.allAssetPricesMapping.get(key)
//										.get(ThruBreaker.accessPriceDateRange(everyday, 0)).macdGoldCrossStatus
//								+ ", boolUp: " + ThruBreaker.allAssetPricesMapping.get(key)
//										.get(ThruBreaker.accessPriceDateRange(everyday, 0)).bollUp
//										+ ", closePrice: " + ThruBreaker.allAssetPricesMapping.get(key)
//										.get(ThruBreaker.accessPriceDateRange(everyday, 0)).closePrice		);
//					}
				// }
				System.out.print("today's MACDBOLL Thru:" + "\n ");
				for (String s : macd_boll.keySet()) {
					System.out.print(macd_boll.get(s) + "\n ");
				}
				for (String s : macd_boll.keySet()) {
					System.out.print(s.substring(2) + "\n ");
				}

			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void run() {
		runControlID = 0;
		MACDBOLLBreakThru();
	}

	public static void main(String[] args) {
		new ThruBreaker();
		MACDBOLLService mc = new MACDBOLLService();
		startDT = LocalDateTime.now();
		mc.start();
	}
}
