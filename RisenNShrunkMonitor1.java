package StockMarket;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.commons.math3.stat.regression.SimpleRegression;

class ComprehensivePrice {

	public Date tradeDate;
	public String stockCode;
	public float openPrice;
	public float closePrice;
	public float highPrice;
	public float lowPrice;
	public float amountShares;
	public float amountDollars;
	public float priceDiff;
	public double changeRate;
	public float MA_5;
	public float MA_10;
	public float MA_20;
	public float MA_30;
	public float MA_60;
	public double totalMarketValue;
	public String stockName;
}

public class RisenNShrunkMonitor1 {

	static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://localhost:20/mysql";
	static final String USER = "root";
	static final String PASS = "";

	static List<LocalDate> simulationDateRange;
	static int todayIndex;
	Map<String, Map<Date, PriceNDesc[]>> allAssetListPricesMapping;
	
	static List<LocalDate> setSimulationDateRange(LocalDate start, LocalDate end) {
		simulationDateRange = new LinkedList<LocalDate>();
		LocalDate tail;

		List<LocalDate> exchangeHolidays = new LinkedList<LocalDate>();
		exchangeHolidays.add(LocalDate.of(2024, 04, 04));
		exchangeHolidays.add(LocalDate.of(2024, 04, 05));
		exchangeHolidays.add(LocalDate.of(2024, 05, 01));
		exchangeHolidays.add(LocalDate.of(2024, 05, 02));
		exchangeHolidays.add(LocalDate.of(2024, 05, 03));
		exchangeHolidays.add(LocalDate.of(2024, 06, 10));
		tail = start;
		simulationDateRange.add(start);
		while (tail.isBefore(end)) {
			tail = tail.plusDays(1);
			if (!(tail.getDayOfWeek().equals(DayOfWeek.SATURDAY) || tail.getDayOfWeek().equals(DayOfWeek.SUNDAY)
					|| exchangeHolidays.contains(tail)))
				simulationDateRange.add(tail);
		}
		System.out.println("Simulation date range is: ");
		ListIterator<LocalDate> iter = simulationDateRange.listIterator();
		while (iter.hasNext())
			System.out.println(iter.next());
		return simulationDateRange;
	}

	 static float[][] MA_trend(Asset asset, double[] slope_requirements, String stockCode ){
		
		float[][] MAMatrix = null;
			MAMatrix= new float[asset.allAssetListPricesMapping.get(stockCode).size()][10];
			int i=0;
			for (java.util.Date d: asset.allAssetListPricesMapping.get(stockCode).keySet()) {
				MAMatrix[i][0]=asset.allAssetListPricesMapping.get(stockCode).get(d)[6].getPrice();
				MAMatrix[i][1]=asset.allAssetListPricesMapping.get(stockCode).get(d)[7].getPrice();
				MAMatrix[i][2]=asset.allAssetListPricesMapping.get(stockCode).get(d)[8].getPrice();
				MAMatrix[i][3]=asset.allAssetListPricesMapping.get(stockCode).get(d)[9].getPrice();
				MAMatrix[i][4]=asset.allAssetListPricesMapping.get(stockCode).get(d)[4].getPrice();
				MAMatrix[i][5]=asset.allAssetListPricesMapping.get(stockCode).get(d)[5].getPrice();
				i++;
			}
		
		
		int rows = MAMatrix.length; // gets the number of rows 
		int columns = MAMatrix[0].length; // gets the number of columns 
		SimpleRegression regression = new SimpleRegression();
		
		for (int row=2; row <MAMatrix.length; row++) {
		   for (int backwards=row; backwards >=0; backwards--) {
				regression.addData(2-backwards, MAMatrix[row][0]);
			}	
			
			double slope = regression.getSlope();
			double intercept = regression.getIntercept();
			MAMatrix[row][6]=(float)slope;
			if (slope > slope_requirements[0]) {
				System.out.println("on "+simulationDateRange.get(row)+", "+stockCode + "'s MA_5 slope is ok: " + String.format("%.2f", slope * 100) + "%. ");	    
			}	 		
			}
				
		regression.clear();
	
		for (int row=4; row <MAMatrix.length; row++) {
		   for (int backwards=row; backwards >=0; backwards--) {
				regression.addData(4-backwards, MAMatrix[row][1]);
			}	
			
			double slope = regression.getSlope();
			double intercept = regression.getIntercept();
			MAMatrix[row][7]=(float)slope;
			if (slope > slope_requirements[1]) {
				System.out.println("on "+simulationDateRange.get(row)+", "+stockCode + "'s MA_10 slope is ok: " + String.format("%.2f", slope * 100) + "%. ");	     
			}
			}
	
		regression.clear();
	
		for (int row=14; row <MAMatrix.length; row++) {
			for (int backwards=row; backwards >=0; backwards--) {
		//asset.trend( MAValues[row], slope_requirements[row], stockCode, stockName) ;			
		//int count = MAValues.length;
			regression.addData(15-backwards, MAMatrix[row][3]);
		}
		double slope = regression.getSlope();
		double intercept = regression.getIntercept();
		MAMatrix[row][9]=(float)slope;
		if (slope > slope_requirements[2]) {
			System.out.println("on "+simulationDateRange.get(row)+", "+stockCode + "'s MA_30 slope is ok: " + String.format("%.2f", slope * 100) + "%. ");	
		}
		}
		return MAMatrix;
	}

	public static void main(String args[]) throws InterruptedException {
		try {
			// STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);
			Connection conn = DriverManager.getConnection(
					"jdbc:mariadb://localhost:20/mysql?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai",
					"root", "");
			// STEP 4: Execute a query
			Statement stmt = conn.createStatement();
			LocalDate since = LocalDate.of(2024, 7, 1);
			LocalDate till = LocalDate.now();
			setSimulationDateRange(since, till);
			Asset asset = new Asset();
			asset.setAllAssetListPricesMapping(since, conn, stmt);

			List<String> candidates= new ArrayList<String>();
			
			
			System.out.println("asset.allAssetListPricesMapping: "+ asset.allAssetListPricesMapping);
			
			LocalDate today = LocalDate.of(2024, 8, 1);

			try {
				LocalDate yesterday;
				todayIndex = simulationDateRange.indexOf(today);
				if (todayIndex - 1 >= 0) {
					yesterday = simulationDateRange.get(todayIndex - 1);
				} else {
					System.out.println("today is the 1st day of simulation.");
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			Map<String, ComprehensivePrice> pm = new HashMap<String, ComprehensivePrice>();
			Map<String, ComprehensivePrice> pm1 = new HashMap<String, ComprehensivePrice>();

			double[] slope_requirements= {0.0,0.0,0.0};
			System.out.println("Risen 'N' Shrunk Monitor on " + today + ":");
			for (String key : asset.allAssetListPricesMapping.keySet()) {
				float[][] MAMatrix = MA_trend(asset, slope_requirements, key);

				int rows = MAMatrix.length; // gets the number of rows 
				int columns = MAMatrix[0].length; // gets the number of columns 
				
				
				for (int i=0; i < MAMatrix.length; i++) {
					for (int j=0; j < MAMatrix[0].length; j++) {
						System.out.print(MAMatrix[i][j]);
						System.out.print(",");
						if (j==MAMatrix[0].length-1) {
							System.out.print("\n");
						}
						}
					}
				
				System.out.println("rows: " + rows );
				System.out.println("columns: " + columns );
				System.out.println("todayIndex " + todayIndex );
				if( todayIndex<rows) {
				if ((MAMatrix[todayIndex][1]-MAMatrix[todayIndex][0]>0) && (MAMatrix[todayIndex-1][4]- MAMatrix[todayIndex][4])>0 && MAMatrix[todayIndex][6]>0) {
					System.out.println("\"" + key + "\",");
					candidates.add(key);
				}
				}
				System.out.println(today+"'s cadidates:  ");
				
				for (String s: candidates)
				{
					System.out.println("\"" + s + "\",");
				}
				
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
	
	
	
	/*
			String selectYesterday = "select * from daily_snapshot where 交易日期 ='2024-08-06'";
			String selectToday = "select * from daily_snapshot where 交易日期 ='2024-08-07'";

	 * ResultSet rs = stmt.executeQuery(selectYesterday); ResultSet rs1 =
	 * stmt.executeQuery(selectToday);
	 * 
	 * Map<String, ComprehensivePrice> pm = new HashMap<String,
	 * ComprehensivePrice>(); Map<String, ComprehensivePrice> pm1 = new
	 * HashMap<String, ComprehensivePrice>();
	 * 
	 * double[] slope_requirements= {0.0,0.0,0.0};
	 * 
	 * while (rs.next()) { ComprehensivePrice cp = new ComprehensivePrice();
	 * cp.tradeDate = rs.getDate("交易日期"); cp.stockCode = rs.getString("股票代码");
	 * cp.stockName = rs.getString("股票名称"); cp.openPrice = rs.getFloat("开盘价");
	 * cp.closePrice = rs.getFloat("收盘价"); cp.highPrice = rs.getFloat("最高价");
	 * cp.lowPrice = rs.getFloat("最低价"); cp.amountShares = rs.getFloat("成交量");
	 * cp.amountDollars = rs.getFloat("成交额"); cp.changeRate = rs.getDouble("涨跌幅");
	 * 
	 * cp.MA_5 = rs.getFloat("MA_5"); cp.MA_10 = rs.getFloat("MA_10"); cp.MA_20 =
	 * rs.getFloat("MA_20"); cp.MA_30 = rs.getFloat("MA_30");
	 * 
	 * //cp.MA_60 = (float)rs.getFloat("MA_60"); cp.MA5Trend = rs.getFloat("MA_5");
	 * cp.MA10Trend = rs.getFloat("MA_10"); cp.MA20Trend = rs.getFloat("MA_20");
	 * cp.MA30Trend = rs.getFloat("MA_30");
	 * 
	 * 
	 * cp.totalMarketValue = rs.getDouble("总市值"); cp.priceDiff = cp.closePrice -
	 * cp.openPrice; pm.put(cp.stockCode, cp); } while (rs1.next()) {
	 * ComprehensivePrice cp = new ComprehensivePrice(); cp.tradeDate =
	 * rs1.getDate("交易日期"); cp.stockCode = rs1.getString("股票代码"); cp.stockName =
	 * rs1.getString("股票名称"); cp.openPrice = rs1.getFloat("开盘价"); cp.closePrice =
	 * rs1.getFloat("收盘价"); cp.highPrice = rs1.getFloat("最高价"); cp.lowPrice =
	 * rs1.getFloat("最低价"); cp.amountShares = rs1.getFloat("成交量"); cp.amountDollars
	 * = rs1.getFloat("成交额"); cp.changeRate = rs1.getDouble("涨跌幅"); cp.MA_5 =
	 * rs1.getFloat("MA_5"); cp.MA_10 = rs1.getFloat("MA_10"); cp.MA_20 =
	 * rs1.getFloat("MA_20"); cp.MA_30 = rs1.getFloat("MA_30"); cp.MA_60 =
	 * rs1.getFloat("MA_60");
	 * 
	 * cp.totalMarketValue = rs1.getDouble("总市值"); cp.priceDiff = cp.closePrice -
	 * cp.openPrice; pm1.put(cp.stockCode, cp); }
	 */
