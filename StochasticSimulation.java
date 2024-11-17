package StockMarket;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.Map.Entry;

import StockMarket.Portfolio.UHaul;

class Criterion {
	static private HashMap<String, String> criteriaTexts = new HashMap<String, String>();
	static {
		criteriaTexts.put("3RD_RISING_PICK", "select * from active_stock_daily where trade_date='");
		criteriaTexts.put("FJQ_PICK", "select * from FJQ_select_stock where select_date='");
	}

	static String getCretiria(String CretiriaKey) {
		return criteriaTexts.get(CretiriaKey);
	}
}

public class StochasticSimulation {
	// JDBC driver name and database URL
	static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://localhost:20/mysql";
	static final String USER = "root";
	static final String PASS = "";
	static Statement stmt;
	static Connection conn;

	public StochasticSimulation() {
		try {
			// STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(
					"jdbc:mariadb://localhost:20/mysql?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai",
					"root", "");
			// STEP 4: Execute a query
			stmt = conn.createStatement();
			System.out.println("conn: " + conn);
			System.out.println("stmt: " + stmt);
		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
		}
	}

	public static void main(String[] args) {
		LocalDateTime startTime = LocalDateTime.now();
		System.out.println("Start local time: " + LocalDateTime.now());
		new StochasticSimulation();
		System.out.println("-----------------------------------------------------");
		LocalDate start = LocalDate.of(2024, 02, 29);
		LocalDate end = LocalDate.of(2024, 06, 06);
		LocalDate initialDate = LocalDate.of(2024, 04, 01);
		java.sql.Date startDate = java.sql.Date.valueOf(start);
		Portfolio batchBuyingStrategyPortfolio = new Portfolio(1000000.0, startDate, "ALL", 10000000000.0, conn, stmt);
		// Portfolio.setAllAssetListPricesMapping(startDate, conn, stmt);
		for (Entry<Date, Map<String, Map<Date, PriceNDesc[]>>> entry : Portfolio.allAssetListPricesMapping.entrySet()) {
			System.out.println("Select date: " + entry.getKey());
			System.out.println("set: " + entry.getValue());
			// System.out.println("asset "+ asset+ ", prices: " );
			for (Map<Date, PriceNDesc[]> m : entry.getValue().values()) {
				System.out.println("priceMap: ");
				System.out.println("priceMap, pricedate, " + m.keySet());
				System.out.println("priceMap, prices, " + m.values());
			}
		}
		Timestamp MonteCarloTimestamp = new Timestamp(System.currentTimeMillis());
			/*String runtype;
			//runtype = "ALLPREVIOUSABPS";
			runtype = "LASTABPONLY";
			UHaul.setHighBound1(1.35);
			UHaul.setLowBound1(0.85);
			UHaul.setHighBound2(1.09);
			UHaul.setLowBound2(0.93);
			UHaul.setHighBoundMA1(2);
			UHaul.setHighBoundMA2(1.5);
			for (int i = 0; i < 100; i++) {
			LocalDateTime startRunTime = LocalDateTime.now();
			Timestamp batchTimestamp = new Timestamp(System.currentTimeMillis());
			double initialInvestment=10000000.0;
			double faultTradeTolerance = Portfolio.initialCashInvestment * 0.015;
			double sellOffThreshold=0;
			double stopLoss = -0.2;
			double retainGain = 0.25;
			double lowerBoundSpeed = -0.03;
			double averageSpeed = 0.02;
			double positionScale=0.3;
			String sellingStrategy="non-MA";
			String intraDayStrategy="HaulingAtHighLow";
			String strategy="BETTERBUYINGDAY";
			boolean bullishPredicted = false;
			int sublistSize=4, vintage=1, volatilityAcceptanceNumber=0;
			batchBuyingStrategyPortfolio = new Portfolio(initialInvestment, startDate, "ALL", 1000000000.0, conn, stmt);
			if (runtype.equals("ALLPREVIOUSABPS")) {
				batchBuyingStrategyPortfolio.runALL(start, end, sublistSize, positionScale, sellOffThreshold, 
						stopLoss, retainGain, lowerBoundSpeed, averageSpeed, vintage,
						strategy, intraDayStrategy, sellingStrategy, volatilityAcceptanceNumber, 
						faultTradeTolerance, bullishPredicted, batchTimestamp, MonteCarloTimestamp, conn, stmt);
			} else if (runtype.equals("LASTABPONLY")) {
				batchBuyingStrategyPortfolio.runLASTONLY(start, end, sublistSize, positionScale, sellOffThreshold, 
						stopLoss, retainGain, lowerBoundSpeed, averageSpeed, vintage,
						strategy, intraDayStrategy, sellingStrategy, volatilityAcceptanceNumber, 
						faultTradeTolerance, bullishPredicted, batchTimestamp, MonteCarloTimestamp, conn, stmt);
			} else {
				System.out.println("Choose runtype(ALLPREVIOUSABPS | LASTABPONLY) please.");
			}
			LocalDateTime endTime = LocalDateTime.now();
			System.out.println(
					i + "th FJQ Simulation duration: " + startRunTime.until(endTime, ChronoUnit.SECONDS) + " Seconds.");
		}
		System.out.println("End local time: " + LocalDateTime.now());
		LocalDateTime endTime = LocalDateTime.now();
		System.out.println(
				"Monte Carlo Simulation duration: " + startTime.until(endTime, ChronoUnit.MINUTES) + " Minutes.");
*/
//	    findRebound():
		new StochasticSimulation();
		LocalDate since = LocalDate.of(2024, 07, 06);
		LocalDate till = LocalDate.now();
//		// LocalDate till = LocalDate.of(2024, 01, 28);
//		LocalDate anchor = LocalDate.of(2024, 04, 20);
		DescendingDateOrder comparator = new DescendingDateOrder();
//		LocalDateTime startFindRebound = LocalDateTime.now();
//		System.out.println("start findRebound time: " + startFindRebound);
//		Map[] reboundAnalysisMaps=Asset.findRebound(since, till, anchor, 0.75, true,
//		comparator, conn, stmt);
		 Asset.tracingUps(since, 30, comparator, conn, stmt);
//		LocalDateTime endFindRebound = LocalDateTime.now();
//		System.out.println("end findRebound time: " + endFindRebound);
//		System.out.println("findRebound simulation duration: "
//				+ startFindRebound.until(endFindRebound, ChronoUnit.MINUTES) + " Minutes.");
		 Asset asset= new Asset(conn, stmt);
		 double targetSlope= 0.015;
		 asset.lowestPriceRegressing(since, till, 4, targetSlope, "SIMPLE");
//		 }
//
//		//buy after first rising  
//		String select_range_trading_dates = "select distinct 交易日期 from daily_snapshot where 交易日期 < '2024-01-30' order by 交易日期 desc limit 1 ;";
//		ResultSet rs;
//		List<Date> tradingDates = new LinkedList<Date>();
//		try {
//			rs = stmt.executeQuery(select_range_trading_dates);
//			while (rs.next()) {
//				Date tradingDate = rs.getDate("交易日期");
//				tradingDates.add(tradingDate);
//			}
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
//		for (Date d : tradingDates) {
//			Asset.firstDayRising(d, null, null, "1030", conn, stmt);
//		}
	}
}
