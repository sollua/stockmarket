package StockMarket;

import java.net.URL;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.commons.io.IOUtils;

public class QuotingService extends Thread {
	static Connection conn;
	static Statement stmt;
	static List<LocalDate> priceDateRange;
	static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
	static final String DB_URL = "jdbc:mariadb://localhost:20/mysql";
	static final String USER = "root";
	static final String PASS = "";
	static Map<String, String> longForms = null;
	LocalDate priceStart = LocalDate.of(2024, 7, 10);
	static LocalDate priceTill = LocalDate.now();
	static LocalDateTime startDT, endDT;
	static int runControlCount=0;
	java.util.Date currentDate = Date.from(LocalDate.now().atStartOfDay(ZoneId.systemDefault()).toInstant());
	URL url, url_listing;

	static List<LocalDate> setPriceDateRange(LocalDate priceStart, LocalDate priceEnd) {
		priceDateRange = new LinkedList<LocalDate>();
		LocalDate tail;
		List<LocalDate> exchangeHolidays = new LinkedList<LocalDate>();
		exchangeHolidays.add(LocalDate.of(2024, 01, 01));
		exchangeHolidays.add(LocalDate.of(2024, 02, 9));
		exchangeHolidays.add(LocalDate.of(2024, 02, 12));
		exchangeHolidays.add(LocalDate.of(2024, 02, 13));
		exchangeHolidays.add(LocalDate.of(2024, 02, 14));
		exchangeHolidays.add(LocalDate.of(2024, 02, 15));
		exchangeHolidays.add(LocalDate.of(2024, 02, 16));
		exchangeHolidays.add(LocalDate.of(2024, 04, 04));
		exchangeHolidays.add(LocalDate.of(2024, 04, 05));
		exchangeHolidays.add(LocalDate.of(2024, 05, 01));
		exchangeHolidays.add(LocalDate.of(2024, 05, 02));
		exchangeHolidays.add(LocalDate.of(2024, 05, 03));
		exchangeHolidays.add(LocalDate.of(2024, 06, 10));
		exchangeHolidays.add(LocalDate.of(2024, 9, 16));
		exchangeHolidays.add(LocalDate.of(2024, 9, 17));
		exchangeHolidays.add(LocalDate.of(2024, 10, 1));
		exchangeHolidays.add(LocalDate.of(2024, 10, 2));
		exchangeHolidays.add(LocalDate.of(2024, 10, 3));
		exchangeHolidays.add(LocalDate.of(2024, 10, 4));
		exchangeHolidays.add(LocalDate.of(2024, 10, 7));
		tail = priceStart;
		priceDateRange.add(priceStart);
		while (tail.isBefore(priceEnd)) {
			tail = tail.plusDays(1);
			if (!(tail.getDayOfWeek().equals(DayOfWeek.SATURDAY) || tail.getDayOfWeek().equals(DayOfWeek.SUNDAY)
					|| exchangeHolidays.contains(tail)))
				priceDateRange.add(tail);
		}
		System.out.println("priceDateRange date range is: ");
		ListIterator<LocalDate> iter = priceDateRange.listIterator();
		while (iter.hasNext())
			System.out.println(iter.next());
		return priceDateRange;
	}

	static Map<String, String> shortToLong(Statement stmt) {
		String selectLongCodes = "select distinct 股票代码 from daily_snapshot where is_index!='Y';";
		ResultSet rs;
		longForms = new HashMap<String, String>();
		String longCode;
		try {
			rs = stmt.executeQuery(selectLongCodes);
			while (rs.next()) {
				longCode = rs.getString("股票代码");
				longForms.put(longCode.substring(2), longCode);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return longForms;
	}

	QuotingService() {
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(
					DB_URL + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai", USER, PASS);
			stmt=conn.createStatement();
		} catch (Exception excp) {
			excp.printStackTrace();
		}
		priceDateRange = setPriceDateRange(priceStart, priceTill);
		shortToLong(stmt);
	}

	public void run() {
		while (true) {
			if (LocalDateTime.now().isAfter(startDT.plusMinutes(1)) || runControlCount==0) {
				try {
					runControlCount++;
					startDT = LocalDateTime.now();
					System.out.println("Starting realtime Quoting at: " + startDT);
					url = new URL("http://a.mairui.club/hsrl/ssjy/all/cd5268626606b8b4ef");
					url_listing = new URL("http://api.mairui.club/hslt/list/cd5268626606b8b4ef");
					String jsonStr = IOUtils.toString(url, "UTF-8");
					jsonStr = jsonStr.replaceAll("\"", "");
					String regex = "[\\}]";
					String[] ssss = jsonStr.split(regex);
					System.out.println("Finished price quoting HTTP streaming at: " + LocalDateTime.now());
					
					for (int i = 0; i < ssss.length - 1; i++) {
						ssss[i] = ssss[i].substring(2);
					}
					String priceDetails[];
					LocalDate ld = null;
					Map<String, Float> priceMap;
					conn.setAutoCommit(false);
					PreparedStatement preparedStmt = conn.prepareStatement(
							"insert into realtimeprice (stock_code, trade_date, open_price, close_price, high_price, low_price, change_rate, amount_shares, amount_dollars, total_MarketValue, five_min_rise, speed) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
					for (int i = 0; i < ssss.length - 1; i++) {
						AssetStatus as = new AssetStatus();
						priceDetails = ssss[i].split(",");
						priceMap = new HashMap<String, Float>();
						for (int j = 0; j < priceDetails.length; j++) {
							if (priceDetails[j].split(":")[0].equals("t")) {
								ld = LocalDate.parse(priceDetails[j].split(":")[1].substring(0, 10));
								
								continue;
							}
							if (priceDetails[j].split(":")[0].equals("dm")) {
								as.stockCode = String.valueOf(priceDetails[j].split(":")[1]);
							}
							priceMap.put(priceDetails[j].split(":")[0],
									Float.parseFloat(priceDetails[j].split(":")[1]));
						}

						LocalDate currentLocalDate = currentDate.toInstant().atZone(ZoneId.systemDefault())
								.toLocalDate();
						ListIterator<LocalDate> li = priceDateRange.listIterator();
						LocalDate cursorDate;
						if (!priceDateRange.contains(currentLocalDate)) {
							while (li.hasNext()) {
								cursorDate = li.next();
								currentDate = Date.from(cursorDate.atStartOfDay(ZoneId.systemDefault()).toInstant());
								if (cursorDate.isAfter(currentLocalDate)) {
									break;
								}
							}
						}
						as.tradeDate = java.sql.Date.valueOf(ld);
						as.stockCode = longForms.get(as.stockCode);
						System.out.println("股票代码: " + as.stockCode);
						System.out.println("交易日期: " + java.sql.Date.valueOf(ld));
						as.openPrice = priceMap.get("o"); as.closePrice = priceMap.get("p");
						as.highPrice = priceMap.get("h"); as.lowPrice = priceMap.get("l");
						as.amountShares = priceMap.get("v"); as.amountDollars = priceMap.get("cje");
						as.changeRate = priceMap.get("pc"); as.fiveMinRise = priceMap.get("fm");
						as.speed = priceMap.get("zs");
						as.totalMarketValue = priceMap.get("sz"); 
						preparedStmt.setString(1, as.stockCode);
						preparedStmt.setDate(2, as.tradeDate);
						preparedStmt.setFloat(3, as.openPrice);
						preparedStmt.setFloat(4, as.closePrice);
						preparedStmt.setFloat(5, as.highPrice);
						preparedStmt.setFloat(6, as.lowPrice);
						preparedStmt.setDouble(7, as.changeRate);
						preparedStmt.setFloat(8, as.amountShares);
						preparedStmt.setFloat(9, as.amountDollars);
						preparedStmt.setDouble(10, as.totalMarketValue);
						preparedStmt.setDouble(11, as.fiveMinRise);
						preparedStmt.setDouble(12, as.speed);
						preparedStmt.addBatch();
					}
					String truncateRealtimePriceData = "truncate RealtimePrice;";
					Statement stmt = conn.createStatement();
					stmt.executeQuery(truncateRealtimePriceData);
					preparedStmt.executeBatch();
					// Commit into real time table
					conn.commit();
					conn.setAutoCommit(true);
					
					jsonStr = IOUtils.toString(url_listing, "UTF-8");
					jsonStr = jsonStr.replaceAll("\"", "");
					regex = "[\\}]";
					ssss = jsonStr.split(regex);
					System.out.println("Finished listing HTTP streaming at: " + LocalDateTime.now());
					for (int i = 0; i < ssss.length - 1; i++) {
						ssss[i] = ssss[i].substring(2);
					}
					String listingDetails[];
					conn.setAutoCommit(false);
					PreparedStatement preparedStmt2 = conn.prepareStatement(
							"insert into listing (stock_code, stock_name, exchange, date) VALUES (?, ?, ?, ?)");
					for (int i = 0; i < ssss.length - 1; i++) {
						AssetStatus as = new AssetStatus();
						listingDetails = ssss[i].split(",");
						for (int j = 0; j < listingDetails.length; j++) {
							if (listingDetails[j].split(":")[0].equals("dm")) {
								as.stockCode = String.valueOf(listingDetails[j].split(":")[1]);
							}
							if (listingDetails[j].split(":")[0].equals("mc")) {
								as.stockName = String.valueOf(listingDetails[j].split(":")[1]);
							}
							if (listingDetails[j].split(":")[0].equals("jys")) {
								as.exchange = String.valueOf(listingDetails[j].split(":")[1]);
							}
						}
						System.out.println("股票代码: " + as.stockCode);
						System.out.println("股票名称: " + as.stockName);
						preparedStmt2.setString(1, as.stockCode);
						preparedStmt2.setString(2, as.stockName);
						preparedStmt2.setString(3, as.exchange);
						preparedStmt2.setDate(4, java.sql.Date.valueOf(LocalDate.now()));
						preparedStmt2.addBatch();
					}
					String truncateListingData = "truncate listing;";
					stmt = conn.createStatement();
					stmt.executeQuery(truncateListingData);
					preparedStmt2.executeBatch();
					// Commit into real time table
					conn.commit();
					conn.setAutoCommit(true);

					LocalDateTime endDT = LocalDateTime.now();
					System.out.println("Ending realtime quoting and reading listig at " + endDT + ", cost "
							+ startDT.until(endDT, ChronoUnit.SECONDS) + " seconds.");
					// Thread.sleep(30000);
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
		}
	}

	public static void main(String[] args) {
		QuotingService qs = new QuotingService();
		startDT = LocalDateTime.now();
		qs.start();
	}
}
