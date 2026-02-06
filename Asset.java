package StockMarket;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.Date;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.apache.commons.io.IOUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math3.stat.correlation.KendallsCorrelation;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import org.json.CDL;
import org.json.JSONArray;

class DescendingDateOrder implements Comparator<Date> {
	public int compare(Date o1, Date o2) {
		Date i1 = (Date) o1;
		Date i2 = (Date) o2;
		return -i1.compareTo(i2);
	}
}

class AssetPrice {
	String stockCode;
	String stockName;
	float closePrice;
}

public class Asset extends Thread {
	private static java.sql.Date consecutiveDate;
	// private static Asset consecutiveStock;
	private static float consecutiveChange;
	private static String consecutiveName;
	private static float consecutiveOepn;
	private static float consecutiveClose;
	List<String> stocks;

	static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://localhost:20/mysql";
	// Database credentials
	static final String USER = "root";
	static final String PASS = "";
	static BlockingQueue<String> bq = new ArrayBlockingQueue<String>(1000);
	static BlockingQueue<String> bq_code_only = new ArrayBlockingQueue<String>(1000);
	static Statement stmt;
	static Connection conn;
	Map<String, Map<Date, PriceNDesc[]>> allAssetListPricesMapping;

	{
		try {
			if (conn == null) {
				// STEP 2: Register JDBC driver
				Class.forName(JDBC_DRIVER);
				conn = DriverManager.getConnection(
						"jdbc:mariadb://localhost:20/mysql?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai",
						"root", "");
				// STEP 4: Execute a query
				stmt = conn.createStatement();
				System.out.println("conn: " + conn);
				System.out.println("stmt: " + stmt);
			}
		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
		}
	}
	int holdingDays;
	String stockCode, stockName;
	Date TradePriceDate;
	LocalDate buyDate, sellDate, selectDate, vintgeDate;

	public LocalDate getVinatgeDate() {
		return vintgeDate;
	}

	public void setVinatgeDate(LocalDate vinatgeDate) {
		this.vintgeDate = vintgeDate;
	}

	static Map<String, String> reboundedStocks = Collections.emptyMap();
	static Map<String, String> notYetReboundedStocks = Collections.emptyMap();
	static Map<String, String> mixedReboundStocks = Collections.emptyMap();
	static SortedMap<Date, List> reportMap = Collections.emptySortedMap();
	static SortedMap<Date, List> mergeMap = Collections.emptySortedMap();
	boolean inPortfolio, offPortfolio, bought, sold, gainOrLoss;
	int continousRising, continousFalling;

	Map<String, Map<Date, PriceNDesc[]>> setAllAssetListPricesMapping(LocalDate start, Connection conn,
			Statement stmt) {
		String select_all_assets = "select * from daily_snapshot where 交易日期 >='" + start
				+ "'  and (is_index='N' or is_index is null)";

		System.out.println("select_all_assets: " + select_all_assets);
		Date selectDate, tradeDate;
		ResultSet rs;
		String stockCode;
		boolean rising, falling;
		float openPrice, closePrice, highPrice, lowPrice, MA_5, MA_10, MA_20, MA_30, MA_60, amountShares, amountDollars;
		PriceNDesc[] priceAndDesc;
		double totalMarketValue, changeRate;
		// double liquidMarketValue;

		Map<Date, PriceNDesc[]> assetPricesMap = null;
		try {
			rs = stmt.executeQuery(select_all_assets);
			while (rs.next()) {
				priceAndDesc = new PriceNDesc[12];
				// selectDate = rs.getDate("select_date");
				tradeDate = rs.getDate("交易日期");
				stockCode = rs.getString("股票代码").substring(2);
				openPrice = rs.getFloat("开盘价");
				closePrice = rs.getFloat("收盘价");
				highPrice = rs.getFloat("最高价");
				lowPrice = rs.getFloat("最低价");
				amountShares = rs.getFloat("成交量");
				amountDollars = rs.getFloat("成交额");
				MA_5 = rs.getFloat("MA_5");
				MA_10 = rs.getFloat("MA_10");
				MA_20 = rs.getFloat("MA_20");
				MA_30 = rs.getFloat("MA_30");
				// MA_60 = rs.getFloat("MA_60");
				changeRate = rs.getDouble("涨跌幅");
				totalMarketValue = rs.getDouble("总市值");
				// liquidMarketValue = rs.getDouble("流通市值");
				if (changeRate > 0.0985)
					rising = true;
				else
					rising = false;
				if (changeRate < -0.0985)
					falling = true;
				else
					falling = false;
				priceAndDesc[0] = new PriceNDesc(openPrice, rising, falling, totalMarketValue);
				priceAndDesc[1] = new PriceNDesc(closePrice, rising, falling, totalMarketValue);
				priceAndDesc[2] = new PriceNDesc(highPrice, rising, falling, totalMarketValue);
				priceAndDesc[3] = new PriceNDesc(lowPrice, rising, falling, totalMarketValue);
				priceAndDesc[4] = new PriceNDesc(amountShares, rising, falling, totalMarketValue);
				priceAndDesc[5] = new PriceNDesc(amountDollars, rising, falling, totalMarketValue);
				priceAndDesc[6] = new PriceNDesc(MA_5, rising, falling, totalMarketValue);
				priceAndDesc[7] = new PriceNDesc(MA_10, rising, falling, totalMarketValue);
				priceAndDesc[8] = new PriceNDesc(MA_20, rising, falling, totalMarketValue);
				priceAndDesc[9] = new PriceNDesc(MA_30, rising, falling, totalMarketValue);
				// priceAndDesc[10] = new PriceNDesc(MA_60, rising, falling, totalMarketValue);
				priceAndDesc[11] = new PriceNDesc(0, rising, falling, totalMarketValue);
				if (allAssetListPricesMapping == null) {
					allAssetListPricesMapping = new HashMap<String, Map<Date, PriceNDesc[]>>();
				}

				if (allAssetListPricesMapping.containsKey(stockCode)) {
					assetPricesMap = allAssetListPricesMapping.get(stockCode);
					assetPricesMap.putIfAbsent(tradeDate, priceAndDesc);
				} else {
					assetPricesMap = new HashMap<Date, PriceNDesc[]>();
					assetPricesMap.put(tradeDate, priceAndDesc);
				}
				allAssetListPricesMapping.putIfAbsent(stockCode, assetPricesMap);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return allAssetListPricesMapping;
	}

	float quantity, selectClose, openPrice, closePrice, highPrice, lowPrice, intradayAveragePrice, dailyChange,
			marketValue, gainSpeed, vol, volRatio, MA_5, MA_10, MA_20, MA_30, MA_60, MAyear;

	public int getContinousFalling(LocalDate priceDate) {
		return continousFalling;
	}

	public void setContinousFalling(int continousFalling) {
		this.continousFalling = continousFalling;
	}

	public int getContinousRising(LocalDate priceDate) {
		return continousRising;
	}

	public void setContinousRising(int continousRising) {
		this.continousRising = continousRising;
	}

	public int getHoldingDays(LocalDate priceDate, List<LocalDate> simulationDateRange) {
		holdingDays = simulationDateRange.indexOf((priceDate)) - simulationDateRange.indexOf(getBuyDate()) + 1;
		return holdingDays;
	}

	public void setHoldingDays(int holdingDays) {
		this.holdingDays = holdingDays;
	}

	public float getQuantity() {
		return quantity;
	}

	float cost, sellPrice, buyPrice;
	double assetRealizedGainLoss, revenue, assetUnrealizedGainLoss, assetChange;
	public String notes;

	public float getGainSpeed(LocalDate priceDate, String openOrClose) {
		assetChange = getAssetUnrealizedChange(priceDate, openOrClose, conn, stmt);
		if (holdingDays >= 2)
			gainSpeed = (float) (assetChange / holdingDays);
		else
			gainSpeed = 0;
		return gainSpeed;
	}

	public void setGainSpeed(float gainSpeed) {
		this.gainSpeed = gainSpeed;
	}

	public float getSelectClose() {
		return selectClose;
	}

	public void setSelectClose(float selectClose) {
		this.selectClose = selectClose;
	}

	public boolean equals(Object b) {
		if (this == b)
			return true;
		if (!(b instanceof Asset) || b == null)
			return false;
		Asset other = (Asset) b;
		if (this.stockCode != null && other.stockCode != null) {
			if (this.stockCode.equals(other.stockCode))
				return true;
			else
				return false;
		} else
			return false;
	}

	@Override
	public int hashCode() {
		// use a prime number and the hash codes of the fields to generate a unique hash
		// code
		return 11 * this.stockCode.hashCode();
	}

	ActualPriceDateSet getPrice(LocalDate priceDate, String priceType, Connection conn, Statement stmt) {
		Date dateOfPrice = new Date();
		ActualPriceDateSet apd = new ActualPriceDateSet();
		double totalMarketValue = 0;
		try {
			System.out.println("priceDate: " + priceDate);
			String get_price_sql = "select * from daily_snapshot where 股票代码 like ('%" + this.stockCode + "')"
					+ " and 交易日期 <= '" + priceDate
					+ "' and (is_index is null or is_index='N') order by 交易日期 desc limit 1 ";
			ResultSet rs = stmt.executeQuery(get_price_sql);
			if (!rs.next()) {
				System.out.println(priceDate + " is NOT a trading day... ");
			} else {
				rs.beforeFirst();
				while (rs.next()) {
					openPrice = rs.getFloat("开盘价");
					closePrice = rs.getFloat("收盘价");
					highPrice = rs.getFloat("最高价");
					lowPrice = rs.getFloat("最低价");
					MA_5 = rs.getFloat("MA_5");
					MA_10 = rs.getFloat("MA_10");
					MA_20 = rs.getFloat("MA_20");
					MA_30 = rs.getFloat("MA_30");
					MA_60 = rs.getFloat("MA_60");
					dateOfPrice = rs.getDate("交易日期");
					totalMarketValue = rs.getDouble("总市值");
				}
			}
		} catch (SQLException se) {
			// Handle errors for JDBC
			se.printStackTrace();
		} catch (Exception e) {
			// Handle errors for Class.forName
			e.printStackTrace();
		} finally {
		}
		if (priceType.equals("OPEN")) {
			System.out.println("return aseet " + this.stockCode + " open price: " + openPrice + " on " + dateOfPrice);
			apd.setActualPriceDate(
					Instant.ofEpochMilli(dateOfPrice.getTime()).atZone(ZoneId.systemDefault()).toLocalDate());
			apd.setActualPrice(openPrice);
			apd.setActualScale(totalMarketValue);
			return apd;
		} else if (priceType.equals("CLOSE")) {
			System.out.println("return asset " + this.stockCode + " close price: " + closePrice + " on " + dateOfPrice);
			apd.setActualPriceDate(
					Instant.ofEpochMilli(dateOfPrice.getTime()).atZone(ZoneId.systemDefault()).toLocalDate());
			apd.setActualPrice(closePrice);
			apd.setActualScale(totalMarketValue);
			return apd;
		} else if (priceType.equals("HIGH")) {
			System.out.println("return asset " + this.stockCode + " close price: " + closePrice + " on " + dateOfPrice);
			apd.setActualPriceDate(
					Instant.ofEpochMilli(dateOfPrice.getTime()).atZone(ZoneId.systemDefault()).toLocalDate());
			apd.setActualPrice(highPrice);
			apd.setActualScale(totalMarketValue);
			return apd;
		} else if (priceType.equals("LOW")) {
			System.out.println("return asset " + this.stockCode + " close price: " + closePrice + " on " + dateOfPrice);
			apd.setActualPriceDate(
					Instant.ofEpochMilli(dateOfPrice.getTime()).atZone(ZoneId.systemDefault()).toLocalDate());
			apd.setActualPrice(lowPrice);
			apd.setActualScale(totalMarketValue);
			return apd;
		} else if (priceType.equals("MA_5")) {
			System.out.println("return asset " + this.stockCode + " close price: " + closePrice + " on " + dateOfPrice);
			apd.setActualPriceDate(
					Instant.ofEpochMilli(dateOfPrice.getTime()).atZone(ZoneId.systemDefault()).toLocalDate());
			apd.setActualPrice(MA_5);
			apd.setActualScale(totalMarketValue);
			return apd;
		} else if (priceType.equals("MA_10")) {
			System.out.println("return asset " + this.stockCode + " close price: " + closePrice + " on " + dateOfPrice);
			apd.setActualPriceDate(
					Instant.ofEpochMilli(dateOfPrice.getTime()).atZone(ZoneId.systemDefault()).toLocalDate());
			apd.setActualPrice(MA_10);
			apd.setActualScale(totalMarketValue);
			return apd;
		} else if (priceType.equals("MA_20")) {
			System.out.println("return asset " + this.stockCode + " close price: " + closePrice + " on " + dateOfPrice);
			apd.setActualPriceDate(
					Instant.ofEpochMilli(dateOfPrice.getTime()).atZone(ZoneId.systemDefault()).toLocalDate());
			apd.setActualPrice(MA_20);
			apd.setActualScale(totalMarketValue);
			return apd;
		} else if (priceType.equals("MA_30")) {
			System.out.println("return asset " + this.stockCode + " close price: " + closePrice + " on " + dateOfPrice);
			apd.setActualPriceDate(
					Instant.ofEpochMilli(dateOfPrice.getTime()).atZone(ZoneId.systemDefault()).toLocalDate());
			apd.setActualPrice(MA_30);
			apd.setActualScale(totalMarketValue);
			return apd;
		} else if (priceType.equals("MA_60")) {
			System.out.println("return asset " + this.stockCode + " close price: " + closePrice + " on " + dateOfPrice);
			apd.setActualPriceDate(
					Instant.ofEpochMilli(dateOfPrice.getTime()).atZone(ZoneId.systemDefault()).toLocalDate());
			apd.setActualPrice(MA_60);
			apd.setActualScale(totalMarketValue);
			return apd;
		} else {
			System.out.println(this.stockCode + ", no price date info: " + dateOfPrice);
			apd.setActualPriceDate(null);
			apd.setActualPrice(-1);
			apd.setActualScale(totalMarketValue);
			return apd;
		}
	}

	Asset(String stockCode) {
		this.stockCode = stockCode;
	}

	Asset() {
	}

	Asset(List<String> stocks) {
		this.stocks = stocks;
	}

	Asset(String stockCode, float quantity) {
		this.stockCode = stockCode;
		this.quantity = quantity;
	}

	Asset(String[] stockName, float quantity) {
		this.quantity = quantity;
		this.stockName = stockName[0];
	}

	Asset(Connection conn, Statement stmt) {
		this.conn = conn;
		this.stmt = stmt;
	}

	Asset(String stockCode, LocalDate buyDate, float quantity, Connection conn, Statement stmt) {
		this.quantity = quantity;
		this.stockCode = stockCode;
		this.buyDate = buyDate;
		this.buyPrice = getPrice(buyDate, "OPEN", conn, stmt).getActualPrice();
	}

	Asset(String stockCode, LocalDate buyDate, float buyPrice, float quantity) {
		this.quantity = quantity;
		this.stockCode = stockCode;
		this.buyDate = buyDate;
		this.buyPrice = buyPrice;
	}

	void setStockName(String stockCode) {
		this.stockName = stockCode;
	};

	void setBuyDate(LocalDate buyDate) {
		this.buyDate = buyDate;
	};

	void setSelectDate(LocalDate selectDate) {
		this.selectDate = selectDate;
	};

	void setSellDate(LocalDate sellDate) {
		this.sellDate = sellDate;
	};

	void setBuyPrice(float buyPrice) {
		this.buyPrice = buyPrice;
	};

	void setSellPrice(float sellPrice) {
		this.sellPrice = sellPrice;
	};

	void setQuantity(float quantity) {
		this.quantity = quantity;
	};

	String getStockCode() {
		return this.stockCode;
	}

	LocalDate getSelectDate() {
		return this.selectDate;
	};

	public LocalDate getBuyDate() {
		return this.buyDate;
	};

	float getCost() {
		// System.out.println("in getCost, "+quantity+", "+buyPrice);
		cost = (float) (buyPrice * quantity);
		return this.cost;
	};

	float getBuyPrice() {
		return this.buyPrice;
	}

	double getRevenue() {
		// System.out.println("in getCost, "+quantity+", "+buyPrice);
		revenue = sellPrice * quantity;
		return this.revenue;
	};

	float getAssetMarketValue(LocalDate priceDate, String atOpenOrClose) {
		if (atOpenOrClose == null)
			atOpenOrClose = "CLOSE";
		marketValue = quantity * getPrice(priceDate, atOpenOrClose, conn, stmt).getActualPrice();
		System.out.println("asset market Value is: " + marketValue);
		System.out.println("asset quantity is: " + quantity);
		return marketValue;
	}

	float getAssetMarketValue(LocalDate priceDate, Connection conn, Statement stmt) {
		marketValue = quantity * getPrice(priceDate, "CLOSE", conn, stmt).getActualPrice();
		return marketValue;
	}

	double getAssetRealizedGainLoss() {
		this.assetRealizedGainLoss = (this.sellPrice - this.buyPrice) * quantity;
		return assetRealizedGainLoss;
	};

	double getAssetUnrealizedGainLoss(LocalDate priceDate, String atOpenOrClose, Connection conn, Statement stmt) {
		double realTimePrice = Portfolio.getAssetPrice(this.stockCode, this.selectDate, priceDate, atOpenOrClose);
		if (realTimePrice > 0) {
			this.assetUnrealizedGainLoss = (realTimePrice - this.buyPrice) * quantity;
			System.out.println("assetUnrealizedGainLoss is: " + assetUnrealizedGainLoss);
			return assetUnrealizedGainLoss;
		} else {
			return 0;
		}
	};

	double getAssetUnrealizedChange(LocalDate priceDate, String atOpenOrClose, Connection conn, Statement stmt) {
		this.assetChange = getAssetUnrealizedGainLoss(priceDate, atOpenOrClose, conn, stmt) / getCost();
		return assetChange;
	};

	public static Map[] findRebound(LocalDate since, LocalDate till, LocalDate anchor, double threshold,
			boolean enRouteNoMatter, DescendingDateOrder comparator, Connection conn, Statement stmt) {
		Date dateOfTrade = new Date();
		Date spanDate, signifyingDay, lowestDay = null, latestMaxDate;
		float change, lowestClose = 0, totalChange = 1, totalChange3 = 1, totalChange6 = 1, openAfterLowest = 0;
		Map<Asset, List> continuityStockMap = new LinkedHashMap<Asset, List>();
		List<Asset> buffer, head;
		List<String> emergingList;
		float latestMaxClose = (float) 0.00;
		double latestMaxCloseThreshold = 0.00;
		float amount = 0;
		float amountV = 0;
		float MV = 0;
		float exchangeRatio = 0;

		boolean printOut = false;
		String latestMaxStockCode = null, latestMaxStockName = null;
		Date reboundStart = null;
		Map<String, String> reboundedStocks = new HashMap<String, String>();
		Map<String, String> notYetReboundedStocks = new HashMap<String, String>();
		Map<String, String> mixedReboundStocks = new HashMap<String, String>();
		SortedMap<Date, List> reportMap = new TreeMap<Date, List>(new DescendingDateOrder());
		SortedMap<Date, List> mergeMap = new TreeMap<Date, List>(new DescendingDateOrder());
		DecimalFormat df = new DecimalFormat("#.##");
		DecimalFormat expf = new DecimalFormat("###,###,###,###");
		ZoneId defaultZoneId = ZoneId.systemDefault();
		if (till == null) {
			till = LocalDate.now();
		}
		try {
			System.out.println("Since: " + since);
			String get_continuity_sql = "select 股票代码, 股票名称, 交易日期, 涨跌幅, 开盘价,收盘价, MA_5, MA_10 from daily_snapshot where 交易日期 > '"
					+ since + "' and 交易日期 <= '" + till
					+ "' and 涨跌幅 > 0.0985 and 股票代码 not like '%bj%' and 股票代码 not like '%ST%' order by 股票代码,交易日期 ";
			ResultSet rs = stmt.executeQuery(get_continuity_sql);
			if (rs.isLast()) {
				System.out.println("since " + since + ", no rebounded seeds... ");
			} else {
				while (rs.next()) {
					Asset consecutiveStock = new Asset(rs.getString("股票代码"));
					consecutiveStock.stockName = rs.getString("股票名称");
					// consecutiveStock.stockCode = rs.getString("股票代码");
					consecutiveName = rs.getString("股票名称");
					consecutiveChange = rs.getFloat("涨跌幅");
					consecutiveDate = rs.getDate("交易日期");
					consecutiveOepn = rs.getFloat("开盘价");
					consecutiveClose = rs.getFloat("收盘价");
					Asset risingDetail = new Asset(rs.getString("股票代码"));
					risingDetail.dailyChange = consecutiveChange;
					risingDetail.TradePriceDate = consecutiveDate;
					risingDetail.stockName = consecutiveName;
					risingDetail.openPrice = consecutiveOepn;
					risingDetail.closePrice = consecutiveClose;
					if (continuityStockMap.containsKey(consecutiveStock)) {
						buffer = continuityStockMap.get(consecutiveStock);
						buffer.add(risingDetail);
						continuityStockMap.put(consecutiveStock, buffer);
					} else {
						head = new LinkedList<Asset>();
						head.add((Asset) risingDetail);
						continuityStockMap.putIfAbsent(consecutiveStock, head);
					}
				}

				for (Entry<Asset, List> a : continuityStockMap.entrySet()) {
					// System.out.println(a.getKey().stockName+"|"+a.getKey().stockCode + ": ");
					List<Asset> values = a.getValue();
					latestMaxDate = Date.from(since.atStartOfDay(defaultZoneId).toInstant());
					for (Asset priceInfo : values) {
						if (priceInfo.closePrice > latestMaxClose) {
							// if (latestMaxDate.before(priceInfo.TradePriceDate)) {
							latestMaxClose = priceInfo.closePrice;
							latestMaxCloseThreshold = latestMaxClose * threshold;
							latestMaxDate = priceInfo.TradePriceDate;
							latestMaxStockCode = priceInfo.stockCode;
							latestMaxStockName = priceInfo.stockName;
						}
					}
					String get_rebound_start = "select 股票代码, 股票名称, 交易日期, 涨跌幅, 开盘价, 收盘价, MA_5, MA_10 from daily_snapshot where 股票代码= '"
							+ a.getKey().stockCode + "' and 交易日期 > '" + latestMaxDate + "' and 交易日期 <= '" + till
							+ "' and 收盘价<=" + latestMaxCloseThreshold + " order by 交易日期 limit 1;";

					// System.out.println(get_rebound_start);
					ResultSet rebound_rs = stmt.executeQuery(get_rebound_start);
					while (rebound_rs.next()) {
						reboundStart = rebound_rs.getDate("交易日期");
						final double reboundOpen = rebound_rs.getDouble("开盘价");
						final java.sql.Date reboundOpenDay = rebound_rs.getDate("交易日期");
						System.out.println("rebound Open price: " + reboundOpen);
						System.out.println("rebound Day: " + reboundOpenDay);
						String get_rebound_trail = "select 股票代码, 股票名称, 交易日期, 涨跌幅, 开盘价, 收盘价, MA_5, MA_10, 成交量, 成交额, 换手率 "
								+ "from daily_snapshot where 股票代码= '" + a.getKey().stockCode + "' and 交易日期 >= '"
								+ reboundStart + "' and 交易日期 <= '" + till + "' order by 交易日期;";
						ResultSet rebound_trail = stmt.executeQuery(get_rebound_trail);
						String find_lowest = "select * from (select 股票代码, 股票名称, 交易日期, 涨跌幅, 开盘价, 收盘价, 成交量, 成交额, 总市值, 换手率 "
								+ "from daily_snapshot " + "where 股票代码= '" + a.getKey().stockCode + "' and 交易日期 >= '"
								+ reboundStart + "' and 交易日期 <= '" + till + "') d "
								+ "join (select min(收盘价) lowest_close from daily_snapshot where 股票代码= '"
								+ a.getKey().stockCode + "' and 交易日期 >= '" + reboundStart + "' and 交易日期 <= '" + till
								+ "') c on d.收盘价=c.lowest_close;";
						ResultSet actual_rebound_rs = stmt.executeQuery(find_lowest);
						while (actual_rebound_rs.next()) {
							lowestDay = actual_rebound_rs.getDate("交易日期");
							lowestClose = actual_rebound_rs.getFloat("lowest_close");
							String tri_days_right_after_lowest = "select 股票代码, 股票名称, 交易日期, 涨跌幅, 开盘价, 收盘价 from daily_snapshot where 股票代码= '"
									+ a.getKey().stockCode + "' and 交易日期> '" + lowestDay + "' order by 交易日期 limit 3";
							String six_days_right_after_lowest = "select 股票代码, 股票名称, 交易日期, 涨跌幅, 开盘价, 收盘价 from daily_snapshot where 股票代码= '"
									+ a.getKey().stockCode + "' and 交易日期> '" + lowestDay + "' order by 交易日期 limit 6";
							ResultSet tri_after_lowest_rs = stmt.executeQuery(tri_days_right_after_lowest);
							ResultSet six_after_lowest_rs = stmt.executeQuery(six_days_right_after_lowest);
							totalChange3 = 1;
							totalChange6 = 1;

							if (!tri_after_lowest_rs.next()) {
								openAfterLowest = 0;
							}
							if (!six_after_lowest_rs.next()) {
								openAfterLowest = 0;
							}
							tri_after_lowest_rs.beforeFirst();
							six_after_lowest_rs.beforeFirst();
							while (tri_after_lowest_rs.next()) {
								if (tri_after_lowest_rs.isFirst())
									openAfterLowest = tri_after_lowest_rs.getFloat("开盘价");
								change = tri_after_lowest_rs.getFloat("涨跌幅");
								System.out.println(tri_after_lowest_rs.getDate("交易日期") + ", 涨跌幅: " + change);
								totalChange3 = (1 + change) * totalChange3;
								System.out.println("totalChange3: " + totalChange3);
							}
							totalChange3 = totalChange3 - 1;
							System.out.println("**************");
							while (six_after_lowest_rs.next()) {
								if (six_after_lowest_rs.isFirst())
									openAfterLowest = six_after_lowest_rs.getFloat("开盘价");
								change = six_after_lowest_rs.getFloat("涨跌幅");
								System.out.println(six_after_lowest_rs.getDate("交易日期") + ", 涨跌幅: " + change);
								totalChange6 = (1 + change) * totalChange6;
								System.out.println("totalChange6: " + totalChange6);
							}
							totalChange6 = totalChange6 - 1;

							if (actual_rebound_rs.isLast()) {
								amount = actual_rebound_rs.getFloat("成交量");
								amountV = actual_rebound_rs.getFloat("成交额");
								MV = actual_rebound_rs.getFloat("总市值");
								exchangeRatio = actual_rebound_rs.getFloat("换手率");
							}

						}
						System.out.println("Lowest Close: " + lowestClose + ", Rebound Open: " + reboundOpen
								+ ", Lowest Day: " + lowestDay + ", Rebound Open Day: " + reboundOpenDay
								// + ", difference(lowestClose-reboundOpen)/reboundOpen: " +
								// df.format((lowestClose-reboundOpen)/reboundOpen * 100) + "%"
								+ ", open after lowest: " + openAfterLowest + ", signifying on " + lowestDay + ": "
								+ openAfterLowest + ", after which, 3-D change: " + df.format(totalChange3 * 100)
								+ "%, 6-D change: " + df.format(totalChange6 * 100) + "%"
								+ ", difference(lowestClose-reboundOpen)/reboundOpen: "
								+ df.format((lowestClose - reboundOpen) / reboundOpen * 100) + "%");

						int title = 0;

						while (rebound_trail.next()) {
							spanDate = rebound_trail.getDate("交易日期");
							// System.out.println(spanDate);
							LocalDate anchorlocalDate = LocalDate.parse(anchor.toString());
							Instant instant = anchorlocalDate.atTime(LocalTime.MIDNIGHT).atZone(ZoneId.systemDefault())
									.toInstant();
							Date anchorDate = Date.from(instant);
							if (spanDate.after(anchorDate)) {
								printOut = true;
							}
						}
						rebound_trail.beforeFirst();

						if (printOut) {
							while (rebound_trail.next()) {
								if (rebound_trail.getFloat("收盘价") / reboundOpen - 1 > 0.01 || enRouteNoMatter) {
									if (title < 1) {
										System.out.println(latestMaxStockCode + "|" + latestMaxStockName
												+ "  REBOUND started on " + reboundOpenDay + " at " + reboundOpen
												+ " FOLLOWING " + latestMaxDate + ", " + latestMaxClose + ": "
												+ rebound_rs.getDate("交易日期") + ", change: "
												+ df.format(rebound_rs.getFloat("涨跌幅") * 100) + "%, open: "
												+ rebound_rs.getFloat("开盘价") + ", close: "
												+ rebound_rs.getFloat("收盘价"));
									}
									title++;
									if (rebound_trail.getFloat("收盘价") / reboundOpen - 1 > 0) {
										reboundedStocks.put(rebound_trail.getString("股票代码"),
												rebound_trail.getString("股票代码") + " | "
														+ rebound_trail.getString("股票名称") + " | Bounced Back! | "
														+ "signifying on " + lowestDay + ", with 3-D change of "
														+ df.format(totalChange3 * 100) + "%, 6-D change of "
														+ df.format(totalChange6 * 100) + "% | " + lowestDay + " | "
														+ openAfterLowest + " | lowestClose: " + lowestClose
														+ ", openAfterLowest: ¥ " + openAfterLowest
														+ ", amount(value): ¥ " + expf.format(amountV) + ", MV: ¥ "
														+ expf.format(MV) + ", reboundOpen: " + reboundOpen
														+ ", difference(lowestClose-reboundOpen)/reboundOpen: "
														+ df.format((lowestClose - reboundOpen) / reboundOpen * 100)
														+ "%");
									} else {
										notYetReboundedStocks.put(rebound_trail.getString("股票代码"),
												rebound_trail.getString("股票代码") + " | "
														+ rebound_trail.getString("股票名称") + "| Not yet back! |"
														+ " signifying on " + lowestDay + ", with 3-D change of "
														+ df.format(totalChange3 * 100) + "%, 6-D change of "
														+ df.format(totalChange6 * 100) + "% | " + lowestDay + " | "
														+ openAfterLowest + " | lowestClose: " + lowestClose
														+ ", openAfterLowest: ¥ " + openAfterLowest
														+ ", amount(value): ¥ " + expf.format(amountV) + ", MV: ¥ "
														+ expf.format(MV) + ", reboundOpen: " + reboundOpen
														+ ", difference(lowestClose-reboundOpen)/reboundOpen: "
														+ df.format((lowestClose - reboundOpen) / reboundOpen * 100)
														+ "%");
									}
									// put in one map to sort automatically
									mixedReboundStocks.put(rebound_trail.getString("股票代码"),
											rebound_trail.getString("股票代码") + " | " + rebound_trail.getString("股票名称")
													+ "| regardless |" + " signifying on " + lowestDay
													+ ", with 3-D change of " + df.format(totalChange3 * 100)
													+ "%, 6-D change of " + df.format(totalChange6 * 100) + "% | "
													+ lowestDay + " | " + openAfterLowest + " | lowestClose: "
													+ lowestClose + ", openAfterLowest: ¥ " + openAfterLowest
													+ ", amount(value): ¥ " + expf.format(amountV) + ", MV: ¥ "
													+ expf.format(MV) + ", reboundOpen: " + reboundOpen
													+ ", difference(lowestClose-reboundOpen)/reboundOpen: "
													+ df.format((lowestClose - reboundOpen) / reboundOpen * 100) + "%");
									System.out.println(rebound_trail.getString("股票代码") + "|"
											+ rebound_trail.getString("股票名称") + ", Rebound trail date/change/price: "
											+ rebound_trail.getDate("交易日期") + ", change: "
											+ df.format(rebound_trail.getFloat("涨跌幅") * 100)
											+ "%, ACCUMULATIVE change: "
											+ df.format((rebound_trail.getFloat("收盘价") / reboundOpen - 1) * 100)
											+ "%, open: " + rebound_trail.getFloat("开盘价") + ", close: "
											+ rebound_trail.getFloat("收盘价") + ", vol: " + rebound_trail.getFloat("成交量")
											+ ", amount: " + rebound_trail.getFloat("成交额") + ", exchange: "
											+ rebound_trail.getFloat("换手率"));
								}
							}
							if (title > 0)
								System.out.println(
										"============================================================================================================");
						}
					}
					printOut = false;
					latestMaxClose = (float) 0;
				}
			}

			for (Map<String, String> m : new Map[] { reboundedStocks, notYetReboundedStocks }) {
				System.out.println("\"" + m.size() + " Stocks around " + anchor + ":\",");
				for (String e : m.keySet()) {
					System.out.println(m.get(e));
					signifyingDay = new SimpleDateFormat("yyyy-MM-dd").parse(m.get(e).split("\\|")[4]);
					emergingList = reportMap.get(signifyingDay);
					if (emergingList == null) {
						emergingList = new LinkedList<String>();
						emergingList.add(m.get(e));
						reportMap.put(signifyingDay, emergingList);
					} else {
						emergingList.add(m.get(e));
						reportMap.put(signifyingDay, emergingList);
					}
					emergingList = null;
				}

				// do the report logging
				for (Date d : reportMap.keySet()) {
					System.out.println("\"Signifying on: " + d + "\",");
					for (Object i : reportMap.get(d)) {
						System.out.println("\"" + (String) i + "\",");
						String fill_out_report = "insert into rebound_report (stock_code, stock_name, status, report, signifying_date, "
								+ "signifying_threshold, open_after_signifying, detail, start,till) " + "values ('"
								+ ((String) i).split("\\|")[0] + "', '" + ((String) i).split("\\|")[1] + "', '"
								+ ((String) i).split("\\|")[2] + "', '" + ((String) i).split("\\|")[3] + "', '"
								+ ((String) i).split("\\|")[4] + "', '" + threshold + "','"
								+ ((String) i).split("\\|")[5] + "','" + ((String) i).split("\\|")[6] + "','" + since
								+ "','" + till + "')";
						stmt.executeUpdate(fill_out_report);
					}
				}
				reportMap = new TreeMap<Date, List>(new DescendingDateOrder());
				System.out.println(
						"******************************************************************************************************************");
			}

			for (String e : mixedReboundStocks.keySet()) {
				signifyingDay = new SimpleDateFormat("yyyy-MM-dd").parse(mixedReboundStocks.get(e).split("\\|")[4]);
				emergingList = mergeMap.get(signifyingDay);
				if (emergingList == null) {
					emergingList = new LinkedList<String>();
					emergingList.add(mixedReboundStocks.get(e));
					mergeMap.put(signifyingDay, emergingList);
				} else {
					emergingList.add(mixedReboundStocks.get(e));
					mergeMap.put(signifyingDay, emergingList);
				}
			}

			int s = 0;
			for (Date d : mergeMap.keySet()) {
				System.out.println("\"Signifying on: " + d + "\",");
				for (Object i : mergeMap.get(d)) {
					System.out.println("\"" + (String) i + "\",");
					s++;
				}
			}
			// simple import formation
			System.out.println("Stock name listing... ");
			for (Date d : mergeMap.keySet()) {
				// System.out.println("\"Signifying on: " + d + "\",");
				for (Object i : mergeMap.get(d)) {
					System.out.println("\"" + ((String) i).split("\\|")[1] + "\",");
					s++;
				}
			}
			System.out.println("Merged stockpile, of " + s + " stocks.");

		} catch (SQLException se) {
			// Handle errors for JDBC
			se.printStackTrace();
		} catch (Exception e) {
			// Handle errors for Class.forName
			e.printStackTrace();
		} finally {
		}
		Map[] maps = new Map[6];
		maps[0] = continuityStockMap;
		maps[1] = reboundedStocks;
		maps[2] = notYetReboundedStocks;
		maps[3] = mixedReboundStocks;
		maps[4] = reportMap;
		maps[5] = mergeMap;
		return maps;
	}

	public static Map<Asset, List<Asset>> tracingUps(LocalDate beginning, long watchWindow, Connection conn,
			Statement stmt) {
		Map<Asset, List<Asset>> watchDog = new HashMap<Asset, List<Asset>>();
		LocalDate end = beginning.plusDays(watchWindow);
		String get_ups_sql = "select 股票代码, 股票名称, 交易日期, 涨跌幅, 开盘价,收盘价, MA_5, MA_10 from daily_snapshot where 交易日期 > '"
				+ beginning + "' and 交易日期 <= '" + end
				+ "' and 涨跌幅 > 0.0985 and 股票代码 not like '%bj%' and 股票代码 not like '%ST%' order by 股票代码,交易日期 ";
		try {
			ResultSet rs = stmt.executeQuery(get_ups_sql);
			if (rs.isLast()) {
				System.out.println("beginning at " + beginning.getDayOfWeek() + ", " + beginning + ", ending at "
						+ end.getDayOfWeek() + ", " + end + ", no rising stock... ");
			} else {
				while (rs.next()) {
					Asset upsStock = new Asset(rs.getString("股票代码"));
					upsStock.stockName = rs.getString("股票名称");
					upsStock.dailyChange = rs.getFloat("涨跌幅");
					upsStock.TradePriceDate = rs.getDate("交易日期");
					upsStock.openPrice = rs.getFloat("开盘价");
					upsStock.closePrice = rs.getFloat("收盘价");
					List<Asset> priceList = watchDog.get(upsStock);
					if (priceList != null && !priceList.isEmpty())
						priceList.add(upsStock);
					else {
						priceList = new LinkedList<Asset>();
						priceList.add(upsStock);
					}
					watchDog.putIfAbsent(upsStock, priceList);
					System.out.println(upsStock.stockCode + " processed...");
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("beginning at " + beginning.getDayOfWeek() + ", " + beginning + ", ending at "
				+ end.getDayOfWeek() + ", " + end + ", following has been up. Watch their trends.");
		for (Asset s : watchDog.keySet())
			System.out.println("\"" + s.stockCode + "|" + s.stockName + "\",");

		return watchDog;
	}

	public Map<Integer, List<String>> stockSlicing(int NoOfSlices, String premises, LocalDate since, LocalDate till) {
		String stockCode, stockName, get_stock_list = null;
		ResultSet list_rs;
		Map<Integer, List<String>> Slices = new HashMap<Integer, List<String>>();
		List<String> slice;
		Integer key = Integer.valueOf(1);
		int NoOfStocks = 0, eachList = 0;
		try {
			if (premises == "ALL")
				get_stock_list = "select distinct 股票代码 from daily_snapshot where 股票代码 not like '%bj%' ";
			else if (premises == "REBOUND")
				get_stock_list = "select 股票代码, 股票名称, 交易日期, 涨跌幅, 开盘价,收盘价, MA_5, MA_10 from daily_snapshot where 交易日期 > '"
						+ since + "' and 交易日期 <= '" + till
						+ "' and 涨跌幅 > 0.0985 and 股票代码 not like '%bj%' and 股票名称 not like '%ST%' order by 股票代码,交易日期 ";
			list_rs = stmt.executeQuery(get_stock_list);
			while (list_rs.next()) {
				NoOfStocks++;
			}
			eachList = (NoOfStocks - NoOfStocks % NoOfSlices) / NoOfSlices;
			list_rs.beforeFirst();
			slice = new LinkedList<String>();
			while (list_rs.next()) {
				stockCode = list_rs.getString("股票代码");
				slice.add(stockCode);
				if (key <= NoOfSlices) {
					if (slice.size() < eachList) {
					} else {
						Slices.put(key, slice);
						slice = new LinkedList<String>();
						key = key + 1;
						// System.out.println("key No. " + key);
					}
				} else if (slice.size() < NoOfStocks % NoOfSlices) {
				} else {
					Slices.put(key, slice);
					break;
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return Slices;
	}

	public List<String> day_riseNShrink(LocalDate investigationDate, int s, double d) {
		float amountShares;
		float thisAmountShares, lastAmountShares;
		Date theDate;
		List<Float> amountSharesList = new LinkedList<Float>();
		List<Date> dateList = new LinkedList<Date>();
		DescriptiveStatistics ds;
		Set<Date> dateSet;
		double percentile;
		Date previousDate;
		ZoneId defaultZoneId = ZoneId.systemDefault();
		Date investigation = Date.from(investigationDate.atStartOfDay(defaultZoneId).toInstant());

		List<String> riseNshrunk = new ArrayList<String>();

		for (Entry<String, Map<Date, PriceNDesc[]>> e : allAssetListPricesMapping.entrySet()) {
			amountSharesList.clear();

			Map<Date, PriceNDesc[]> em = e.getValue();
			Collection<PriceNDesc[]> a = em.values();
			int j, k = 0;
			PriceNDesc[][] aa = new PriceNDesc[a.size()][12];

			for (PriceNDesc[] emi : a) {
				for (j = 0; j < 12; j++) {
					aa[k][j] = emi[j];
				}
				k++;
			}

			for (j = 0; j < aa.length; j++) {
				amountSharesList.add(Float.valueOf(aa[j][4].getPrice()));
			}

			double[] initialDoubleArray = new double[aa.length];

			for (j = 0; j < aa.length; j++) {
				initialDoubleArray[j] = aa[j][4].getPrice();
				amountSharesList.add(Float.valueOf(aa[j][4].getPrice()));
			}

			ds = new DescriptiveStatistics(initialDoubleArray);

			/*
			 * for ( j = 0; j < aa.length; j++) { amountShares = (float)
			 * aa[j][4].getPrice(); percentile = ds.getPercentile(amountShares);
			 * aa[j][11].setPrice((float) percentile); }
			 */

			dateSet = em.keySet();
			for (Date di : dateSet) {
				dateList.add(di);
			}

			Collections.sort(dateList, new DescendingDateOrder());
			// dateList.indexOf();
			int indexOfDate = dateList.indexOf(investigation);

			previousDate = dateList.get(indexOfDate + 1);

			try {
				lastAmountShares = em.get(previousDate)[4].getPrice();
				thisAmountShares = em.get(investigation)[4].getPrice();
				if (lastAmountShares > 2 * thisAmountShares) {
					riseNshrunk.add(e.getKey());
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}

		for (String rns : riseNshrunk) {
			System.out.println("found stock: " + rns);
		}
		return riseNshrunk;
	}

	public void lowestPriceRegressing(LocalDate since, LocalDate till, int regressionSpan, double slopeThreshold,
			String type) {
		String stockCode, stockName;
		java.sql.Date lowestDay;
		double lowestClose, lastDayPrice = 0;
		int numberOfDays;
		double[] regressionPrices;
		ListIterator<String> li = this.stocks.listIterator();
		int rep = 1;
		LocalDateTime mid1, mid2;
		try {
			while (li.hasNext()) {
				mid1 = mid2 = null;
				stockCode = li.next();
				numberOfDays = 0;

				LocalDateTime start = LocalDateTime.now();
				System.out.println("start = " + start);
				System.out.println("stocks: " + stocks.size() + ", thread: " + this.getName());
				System.out.println("stock: " + stockCode + ", thread: " + this.getName());
				System.out.println("rep: " + rep + ", " + String.format("%.2f", (float) rep / stocks.size() * 100)
						+ "%, thread: " + this.getName());
				regressionPrices = new double[regressionSpan];
				if (type.equals("SIMPLE")) {
					stockName = " ";
					String days_simple = "select 股票代码, 股票名称, 交易日期, 涨跌幅, 开盘价, 收盘价 from daily_snapshot where 股票代码= '"
							+ stockCode + "' order by 交易日期 desc limit " + regressionSpan;
					ResultSet days_simple_rs = stmt.executeQuery(days_simple);
					numberOfDays = 0;
					while (days_simple_rs.next()) {
						numberOfDays++;
					}
					if (numberOfDays >= regressionSpan) {
						days_simple_rs.beforeFirst();
						int i = 0;
						while (days_simple_rs.next()) {
							if (i == 0)
								lastDayPrice = days_simple_rs.getDouble("收盘价") * 100;
							regressionPrices[i] = days_simple_rs.getDouble("收盘价") / lastDayPrice * 100;
							stockName = days_simple_rs.getString("股票名称");
							i++;
						}
						mid2 = LocalDateTime.now();
						System.out.println("mid 2 = " + mid2);
						trend(regressionPrices, slopeThreshold, stockCode, stockName);
					}
				} else {
					String find_lowest = "select * from (select 股票代码, 股票名称, 交易日期, 涨跌幅, 开盘价, 收盘价 from daily_snapshot  "
							+ "where 股票代码= '" + stockCode + "' and 交易日期 >= '" + since + "' and 交易日期 <= '" + till
							+ "') d " + "join (select min(收盘价) lowest_close from daily_snapshot where 股票代码= '"
							+ stockCode + "' and 交易日期 >= '" + since + "' and 交易日期 <= '" + till
							+ "') c on d.收盘价=c.lowest_close;";
					ResultSet find_lowest_rs = stmt.executeQuery(find_lowest);

					mid1 = LocalDateTime.now();
					System.out.println("mid 1 duration: " + start.until(mid1, ChronoUnit.SECONDS) + " SEC.");
					while (find_lowest_rs.next()) {
						stockName = find_lowest_rs.getString("股票名称");
						lowestDay = find_lowest_rs.getDate("交易日期");
						lowestClose = find_lowest_rs.getFloat("lowest_close");
						String days_after_lowest = "select 股票代码, 股票名称, 交易日期, 涨跌幅, 开盘价, 收盘价 from daily_snapshot where 股票代码= '"
								+ stockCode + "' and 交易日期 > '" + lowestDay + "' order by 交易日期 desc limit "
								+ regressionSpan;
						ResultSet days_after_lowest_rs = stmt.executeQuery(days_after_lowest);
						numberOfDays = 0;
						while (days_after_lowest_rs.next()) {
							numberOfDays++;
						}
						if (numberOfDays >= regressionSpan) {
							days_after_lowest_rs.beforeFirst();
							int i = 0;
							while (days_after_lowest_rs.next()) {
								if (i == 0)
									lastDayPrice = days_after_lowest_rs.getDouble("收盘价") * 100;
								regressionPrices[i] = days_after_lowest_rs.getDouble("收盘价") / lastDayPrice * 100;
								stockName = days_after_lowest_rs.getString("股票名称");
								i++;
							}
							mid2 = LocalDateTime.now();
							System.out.println("mid 2 = " + mid2);
							trend(regressionPrices, slopeThreshold, stockCode, stockName);
						}
					}
				}
				rep++;
				LocalDateTime end = LocalDateTime.now();
				System.out.println("end = " + end);
				if (mid2 != null)
					System.out.println("mid-end duration: " + mid2.until(end, ChronoUnit.SECONDS) + " SEC.");
				System.out.println("1 stock duration: " + start.until(end, ChronoUnit.SECONDS) + " SEC.");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void flatPriceRegressing(LocalDate since, LocalDate till, int regressionSpan, double range) {
		String stockCode, stockName;
		java.sql.Date tradeDay;
		double lowestClose, lastDayPrice = 0;
		int numberOfDays;
		double[] regressionPrices;
		ListIterator<String> li = this.stocks.listIterator();
		int rep = 1;
		LocalDateTime mid1, mid2;
		try {
			while (li.hasNext()) {
				mid1 = mid2 = null;
				stockCode = li.next();
				numberOfDays = 0;
				LocalDateTime start = LocalDateTime.now();
				System.out.println("start = " + start);
				System.out.println("stocks: " + stocks.size() + ", thread: " + this.getName());
				System.out.println("stock: " + stockCode + ", thread: " + this.getName());
				System.out.println("rep: " + rep + ", " + String.format("%.2f", (float) rep / stocks.size() * 100)
						+ "%, thread: " + this.getName());
				String recent_price = "select 股票代码, 股票名称, 交易日期, 涨跌幅, 开盘价, 收盘价 from daily_snapshot  " + "where 股票代码= '"
						+ stockCode + "' and 交易日期 >= '" + since + "' and 交易日期 <= '" + till
						+ "' order by 交易日期 desc limit " + regressionSpan;
				ResultSet recent_price_rs = stmt.executeQuery(recent_price);
				regressionPrices = new double[regressionSpan];
				mid1 = LocalDateTime.now();
				System.out.println("mid 1 duration: " + start.until(mid1, ChronoUnit.SECONDS) + " SEC.");
				int i = 0;
				while (recent_price_rs.next()) {
					stockName = recent_price_rs.getString("股票名称");
					tradeDay = recent_price_rs.getDate("交易日期");
					if (i == 0)
						lastDayPrice = recent_price_rs.getDouble("收盘价");
					regressionPrices[i] = recent_price_rs.getDouble("收盘价") / lastDayPrice;
					i++;
					mid2 = LocalDateTime.now();
					System.out.println("mid 2 = " + mid2);
				}
				recent_price_rs.beforeFirst();
				if (recent_price_rs.next())
					flatTrend(regressionPrices, stockCode, range);
				rep++;
				LocalDateTime end = LocalDateTime.now();
				System.out.println("end = " + end);
				if (mid2 != null)
					System.out.println("mid-end duration: " + mid2.until(end, ChronoUnit.SECONDS) + " SEC.");
				System.out.println("1 stock duration: " + start.until(end, ChronoUnit.SECONDS) + " SEC.");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void trend(double[] values, double slopeThreshold, String stockCode, String stockName) {
		SimpleRegression regression = new SimpleRegression();
		int count = values.length;
		for (int i = 1; i <= count; i++) {
			regression.addData(i, values[count - i]);
		}
		double slope = regression.getSlope();
		double intercept = regression.getIntercept();
		if (slope > slopeThreshold) {
			for (int i = 1; i <= count; i++) {
				System.out.println("value: " + values[count - i]);
			}
			System.out.println(stockCode + "'s slope is positive: " + String.format("%.2f", slope * 100) + "%. - "
					+ this.getName());
			bq.add(stockCode + "|" + stockName);
			bq_code_only.add(stockCode);
		}
	}

	public void trend(double[] values, double slopeThreshold) {
		SimpleRegression regression = new SimpleRegression();
		int count = values.length;
		for (int i = 1; i <= count; i++) {
			regression.addData(i, values[count - i]);
		}
		double slope = regression.getSlope();
		double intercept = regression.getIntercept();
		if (slope > slopeThreshold) {
			for (int i = 1; i <= count; i++) {
				System.out.println("value: " + values[count - i]);
			}
		}
	}

	public double trend(double[] values, String param) {
		SimpleRegression regression = new SimpleRegression();
		int count = values.length;
		for (int i = 0; i < count; i++) {
			regression.addData(i, values[i]);
		}
		double slope = regression.getSlope();
		double intercept = regression.getIntercept();
		double[] r = { slope, intercept };
		if (param.equals("slope")) {
			return r[0];
		} else if (param.equals("intecept")) {
			return r[1];
		} else
			return r[0];
	}

	public double mean(double[] values) {
		DescriptiveStatistics ds = new DescriptiveStatistics(values);
		double mean = ds.getMean();
		return mean;
	}

	public double[] trend(Double[] values) {
		SimpleRegression regression = new SimpleRegression();
		int count = values.length;
		for (int i = 0; i < count; i++) {
			regression.addData(i, values[i]);
		}
		double slope = regression.getSlope();
		double intercept = regression.getIntercept();
		double[] r = { slope, intercept };
		return r;
	}

	public void flatTrend(double[] values, String stockCode, double range) {
		SimpleRegression regression = new SimpleRegression();
		int count = values.length;
		for (int i = 1; i <= count; i++) {
			regression.addData(i, values[count - i]);
		}
		double slope = regression.getSlope();
		double intercept = regression.getIntercept();
		if (slope < range && slope > -range) {
			for (int i = 1; i <= count; i++) {
				System.out.println("value: " + values[count - i]);
			}
			System.out.println(
					stockCode + "'s slope is flat: " + String.format("%.2f", slope * 100) + "%. - " + this.getName());
			bq.add(stockCode + "|" + stockName);
			bq_code_only.add(stockCode);
		}
	}

	public double standardizedVolatility(double[] values) {
		double[] tmp = new double[values.length];
		DescriptiveStatistics ds = new DescriptiveStatistics(values);
		double mean = ds.getMean();
		if (mean != 0) {
			for (int i = 0; i < values.length; i++) {
				tmp[i] = values[i] / mean;
			}
		} else {
			for (int i = 0; i < values.length; i++) {
				tmp[i] = 0;
			}
		}

		DescriptiveStatistics ds1 = new DescriptiveStatistics(tmp);
		double volatility = ds1.getStandardDeviation();
		return volatility;
	}

	public double queryVolatility(java.util.Date date, String key, String volatilityStr, Statement stmt) {
		String queryVolatility = "select volatility_60_standardized from volatility where stock_code='"+key+"'"+" and trade_date ='"+date+"'";
		double volatility = 0;
		try {
			ResultSet rs = stmt.executeQuery(queryVolatility);
		
		while(rs.next()) {
			volatility = rs.getDouble("volatilityStr");
				volatility = rs.getDouble("volatility_60_standardized");
		}
		} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		return volatility;
	}
	
	
	public double numericVolatility(double[] values) {
		double[] tmp = new double[values.length];
		DescriptiveStatistics ds = new DescriptiveStatistics(values);
		/*
		 * double mean = ds.getMean(); for (int i =0; i < values.length; i++) { tmp[i] =
		 * values[i] / mean; }
		 * 
		 * DescriptiveStatistics ds1 = new DescriptiveStatistics(tmp);
		 */
		double volatility = ds.getStandardDeviation();
		return volatility;
	}

	public double PearsonCorrelation(double[] prices1, double[] prices2) {
		PearsonsCorrelation pearson = new PearsonsCorrelation();
		return pearson.correlation(prices1, prices2);
	}

	public double KendallCorrelation(double[] prices1, double[] prices2) {
		KendallsCorrelation kendall = new KendallsCorrelation();
		return kendall.correlation(prices1, prices2);
	}

	public static void firstDayRising(Date d, Date since, Date till, String window, Connection conn, Statement stmt) {
		Date previousTradingDate = null, nextTradingDate = null;
		List<String> rising = new LinkedList<String>(), previousRising = new LinkedList<String>(),
				acceptableList = new LinkedList<String>();
		List<AssetPrice> priceDetailList = new LinkedList<AssetPrice>(),
				previousPriceDetailList = new LinkedList<AssetPrice>();
		String get_stock_list_on_previous_day = null;
		String get_stock_list_on_a_single_day = null;

		double open = 0.0, high = 0.0, low = 0.0, close = 0.0;
		if (window.equals("1030")) {
			window = " 10:30";
		} else if (window.equals("1130")) {
			window = " 11:30";
		} else {
			window = " 10:30";
		}
		String get_stock_list = "select 股票代码, 股票名称, 交易日期, 涨跌幅, 开盘价, 收盘价, MA_5, MA_10 from daily_snapshot where 交易日期 > '"
				+ since + "' and 交易日期 <= '" + till
				+ "' and 涨跌幅 > 0.0985 and 股票代码 not like '%bj%' and 股票名称 not like '%ST%' order by 股票代码,交易日期;";

		get_stock_list_on_a_single_day = "select 股票代码, 股票名称, 交易日期, 涨跌幅, 开盘价, 收盘价, MA_5, MA_10 from daily_snapshot where 交易日期 = '"
				+ d + "' and 涨跌幅 > 0.099 and 股票代码 not like '%bj%' and 股票名称 not like '%ST%' and 股票代码 not like '%300%' "
				+ " union " + "select 股票代码, 股票名称, 交易日期, 涨跌幅, 开盘价,收盘价, MA_5, MA_10 from daily_snapshot where 交易日期 = '"
				+ d + "' and 涨跌幅 > 0.198 and 股票代码 not like '%bj%' and 股票名称 not like '%ST%' and 股票代码 like '%300%' "
				+ "order by 股票代码,交易日期";

		String get_previous_trading_date = "select 交易日期 from daily_snapshot where 交易日期 < '" + d
				+ "' order by 交易日期 desc limit 1;";
		String get_next_trading_date = "select 交易日期 from daily_snapshot where 交易日期 > '" + d
				+ "' order by 交易日期  limit 1;";
		try {
			ResultSet rs1 = stmt.executeQuery(get_previous_trading_date);
			while (rs1.next()) {
				previousTradingDate = rs1.getDate("交易日期");
			}
			System.out.println("get_previous_trading_date: " + get_previous_trading_date);
			System.out.println("previousTradingDate: " + previousTradingDate);
			ResultSet rs2 = stmt.executeQuery(get_next_trading_date);
			while (rs2.next()) {
				nextTradingDate = rs2.getDate("交易日期");
			}

			System.out.println("get_next_trading_date: " + get_next_trading_date);
			System.out.println("nextTradingDate: " + nextTradingDate);
			get_stock_list_on_previous_day = "select 股票代码, 股票名称, 交易日期, 涨跌幅, 开盘价, 收盘价, MA_5, MA_10 from daily_snapshot where 交易日期 = '"
					+ previousTradingDate
					+ "' and 涨跌幅 > 0.099 and 股票代码 not like '%bj%' and 股票名称 not like '%ST%' and 股票代码 not like '%300%' "
					+ " union "
					+ "select 股票代码, 股票名称, 交易日期, 涨跌幅, 开盘价,收盘价, MA_5, MA_10 from daily_snapshot where 交易日期 = '"
					+ previousTradingDate
					+ "' and 涨跌幅 > 0.198 and 股票代码 not like '%bj%' and 股票名称 not like '%ST%' and 股票代码 like '%300%' "
					+ " order by 股票代码,交易日期";

			ResultSet rs_previous_day = stmt.executeQuery(get_stock_list_on_previous_day);
			ResultSet rs = stmt.executeQuery(get_stock_list_on_a_single_day);
			if (rs.isLast()) {
				System.out.println("day " + d + ", has no rising stock... ");
			} else {
				while (rs.next()) {
					rising.add(rs.getString("股票代码"));
					AssetPrice a = new AssetPrice();
					a.stockCode = rs.getString("股票代码");
					a.stockName = rs.getString("股票名称");
					a.closePrice = rs.getFloat("收盘价");
					priceDetailList.add(a);
				}

				if (rs_previous_day.isLast()) {
					System.out.println("previous day " + d + ", has no rising stock... ");
				} else {
					while (rs_previous_day.next()) {
						previousRising.add(rs_previous_day.getString("股票代码"));
					}
				}
			}
		} catch (SQLException s) {
			System.out.println(s);
		}
		for (String s : previousRising) {
			if (rising.contains(s)) {
				rising.remove(s);
			}
		}
		System.out.println("rising contains: " + rising.size());
		System.out.println("get_stock_list_on_previous_day: " + get_stock_list_on_previous_day);
		System.out.println("get_stock_list_on_a_single_day: " + get_stock_list_on_a_single_day);

		/*
		 * try { sleep(100000); } catch (InterruptedException e) { // TODO
		 * Auto-generated catch block e.printStackTrace(); }
		 */
		int count = 0;
		for (String s : rising) {
			URL url = null;
			String json = null;
			try {
				url = new URL(
						"http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol="
								+ s + "&scale=60&ma=no&datalen=255");
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				json = IOUtils.toString(url, "UTF-8");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println(json);
			String[] hourlyPrices = json.split("day");
			for (int i = 0; i < hourlyPrices.length; i++) {
				System.out.println(hourlyPrices[i]);
				String[] hourlyPriceArray = hourlyPrices[i].split(",");
				for (int j = 0; j < hourlyPriceArray.length; j++) {
					if (hourlyPriceArray[0].contains(nextTradingDate.toString() + window)) {
						open = Double.valueOf(hourlyPriceArray[1].split(":")[1].replace("\"", ""));
						high = Double.valueOf(hourlyPriceArray[2].split(":")[1].replace("\"", ""));
						low = Double.valueOf(hourlyPriceArray[3].split(":")[1].replace("\"", ""));
						close = Double.valueOf(hourlyPriceArray[4].split(":")[1].replace("\"", ""));
						System.out.println("stockCode " + s);
						System.out.println("open " + open);
						System.out.println("close " + close);
					}
				}
			}

			for (AssetPrice as : priceDetailList) {
				if (as.stockCode.equals(s)) {
					// System.out.println(s+ "as.closePrice "+as.closePrice+ " close "+close+" open
					// "+open);
					if (as.closePrice < open || as.closePrice < close) {
						System.out.println("as.closePrice " + as.closePrice);
						System.out.println("open " + open);
						System.out.println("close " + close);

						acceptableList.add(s);
						String for_persistence_1030 = "insert into first_rising (stock_code, stock_name, first_rising_date, close_price, next_day_1030_open, next_day_1030_close) "
								+ " values ('" + s + "', '" + as.stockName + "', '" + d + "', " + as.closePrice + ", "
								+ open + ", " + close + ");";
						String for_persistence_1130 = "insert into first_rising (stock_code, stock_name, first_rising_date, close_price, next_day_1130_open, next_day_1130_close) "
								+ " values ('" + s + "', '" + as.stockName + "', '" + d + "', " + as.closePrice + ", "
								+ open + ", " + close + ");";
						if (window.trim().equals("10:30")) {
							try {
								System.out.println("for_persistence_1030: " + for_persistence_1030);
								stmt.executeQuery(for_persistence_1030);
							} catch (SQLException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						} else if (window.trim().equals("11:30")) {
							try {
								System.out.println("for_persistence_1130: " + for_persistence_1130);
								stmt.executeQuery(for_persistence_1130);
							} catch (SQLException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						} else {
						}
					}
				}
			}
			count++;
			if (count % 100 == 0) {
				try {
					sleep(60000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		System.out.println(previousTradingDate + ", previous day rising stocks: ");
		for (String s : previousRising) {
			System.out.println("\"" + s + "\",");
		}

		System.out.println(d + ", first day rising stocks: ");
		for (String s : rising) {
			System.out.println("\"" + s + "\",");
		}

		System.out.println(nextTradingDate + ", next day ACCEPTABLE rising stocks: ");
		for (String s : acceptableList) {
			System.out.println("\"" + s + "\",");
		}
	}

	@Override
	public void run() {
		LocalDate since = LocalDate.of(2024, 07, 01);
		LocalDate till = LocalDate.now();
		LocalDate investigationDate = LocalDate.of(2024, 07, 24);
		int upwardRegressionSpan = 4;
		// int flatRegressionSpan = 35;
		System.out.println("Start running thread: " + this.getName());
		for (String s : stocks)
			System.out.print(s + ", " + "\t");

		lowestPriceRegressing(since, till, upwardRegressionSpan, 0.015, "SIMPLE");
		// setAllAssetListPricesMapping(since, conn, stmt);
		// day_riseNShrink( investigationDate, 0, 0.0);

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
			System.out.println("conn: " + conn);
			System.out.println("stmt: " + stmt);

			LocalDate since = LocalDate.of(2024, 07, 01);
			LocalDate till = LocalDate.now();
			int NoOfSlices = 50;
			List<String> l = new LinkedList<String>();
			l.add("sz300909");
			Asset a = new Asset(l);
			till = LocalDate.now();
			Map<Integer, List<String>> stockSlices = a.stockSlicing(NoOfSlices, "ALL", since, till);
			Asset[] assets = new Asset[NoOfSlices + 1];
			LocalDateTime parallelingStart = LocalDateTime.now();
			String code_only, code_only_short, code_ths;
			System.out.println("start parallel processing time: " + parallelingStart);
			for (Integer key : stockSlices.keySet()) {
				assets[key - 1] = new Asset(stockSlices.get(key));
				assets[key - 1].start();
			}
			for (Integer key : stockSlices.keySet()) {
				while (assets[key - 1].isAlive() == true)
					sleep(300);
				System.out.println(assets[key - 1] + ".isAlive();");
			}
			System.out.println("code|name as:");
			Iterator<String> bq_iterator = bq.iterator();
			while (bq_iterator.hasNext()) {
				System.out.println("\"" + bq_iterator.next() + "\",");
			}

			System.out.println("name only as:");
			Iterator<String> bq_code_only_iterator = bq_code_only.iterator();
			while (bq_code_only_iterator.hasNext()) {
				code_only = bq_code_only_iterator.next();
				code_only_short = code_only.substring(2);
				/*
				 * 1、创业板 创业板的代码是300打头的股票代码；-- 2、沪市A股 沪市A股的代码是以600、601或603打头； 3、沪市B股
				 * 沪市B股的代码是以900打头； 4、深市A股 深市A股的代码是以000打头；-- 5、中小板 中小板的代码是002打头； 6、深圳B股
				 * 深圳B股的代码是以200打头； --
				 */
				if (code_only_short.startsWith("30") || code_only_short.startsWith("000")
						|| code_only_short.startsWith("200")) {
					code_ths = "0" + code_only_short;
				} else if (code_only_short.startsWith("601") || code_only_short.startsWith("602")
						|| code_only_short.startsWith("603")) {
					code_ths = "1" + code_only_short;
				} else {
					code_ths = code_only_short;
				}
				System.out.println(code_ths + ",");
				String insert_into_upward = "insert into upward (股票代码) " + "values ('" + code_only + "');";
				stmt.executeUpdate(insert_into_upward);
			}
			System.out.println("paralleling done.");
			LocalDateTime parallelingEnd = LocalDateTime.now();
			System.out.println("Paralleling duration: " + parallelingStart.until(parallelingEnd, ChronoUnit.MINUTES)
					+ " Minutes.");

		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
		}

	}

}
