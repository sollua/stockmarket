package StockMarket;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.DayOfWeek;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.Stack;
import java.util.Vector;

public class Portfolio {
	List<Asset> assetList;
	List<Asset> statementList;
	float consolidatedBuyPrice, consolidatedSellPrice, openPrice, closePrice, highPrice, lowPrice,
			consolidatedIntradayAveragePrice, consolidatedChange, consolidatedHoldingChange, consolidatedGainSpeed;
	double unrealizedGainLoss, realizedGainLoss, securitiesMarketValue, initialTotalAsset, cashBalance,
			totalAssetMarketValue;
	boolean consolidatedGainOrLossSign;
	int transactionNo;
	private List<LocalDate> simulationDateRange;
	Date portfolioInitialDate;
	static double initialCashInvestment;
	static Connection conn;
	static Statement stmt;
	static Map<Date, Map<String, Map<Date, PriceNDesc[]>>> allAssetListPricesMapping;
	static Map<String, Double> allAssetListFeatureMapping;

	static Map<LocalDate, AssetBuyingPlan> vintageSchedule = new HashMap<LocalDate, AssetBuyingPlan>();

	class TradingPoint {
		String stockCode;
		String stockName;
		Date buyDate;
		Date sellDate;
		double buyingPrice = 0.0;
		double sellingPriceHigh = 0.0;
		double sellingPriceClose = 0.0;
		double sellingPriceOpen = 0.0;
		double sellingPriceHighLowAverage = 0.0;
		double gainRateOpen = 0.0;
		double gainRateHigh = 0.0;
		double gainRateClose = 0.0;
		double gainRateHighLowAverage = 0.0;
	}

	  static class  UHaul{
		 static double highBound1;
		 static double highBound2;
		 static double highBoundMA1;
		 static double highBoundMA2;
		 static double lowBoundMA;
		 public static double getHighBound2() {
			return highBound2;
		}
		public static void setHighBound2(double highBound2) {
			UHaul.highBound2 = highBound2;
		}
		public static double getLowBound2() {
			return lowBound2;
		}
		public static void setLowBound2(double lowBound2) {
			UHaul.lowBound2 = lowBound2;
		}
		public static void setLowBound1(double lowBound1) {
			UHaul.lowBound1 = lowBound1;
		}
		public static double getHighBound1() {
			return highBound1;
		}
		public static void setHighBound1(double highBound1) {
			UHaul.highBound1 = highBound1;
		}
		static double lowBound1;
		static double lowBound2;
		public static double getLowBound1() {
			return lowBound1;
		}
		public static void setLowBound(double lowBound1) {
			UHaul.lowBound1 = lowBound1;
		}
		public static double getHighBoundMA1() {
			return highBoundMA1;
		}
		public static void setHighBoundMA1(double highBoundMA1) {
			UHaul.highBoundMA1 = highBoundMA1;
		}
		public static double getHighBoundMA2() {
			return highBoundMA2;
		}
		public static void setHighBoundMA2(double highBoundMA2) {
			UHaul.highBoundMA2 = highBoundMA2;
		}
	 }
	public class TransactionRecord {
		float sellQuantity, buyQuantity, selectClose, cost, sellPrice, buyPrice;
		double revenue, RGLRate, UGLRate, RGL, UGL;
		LocalDate sellDate, buyDate, selectDate;
		Asset asset;
		Timestamp batchTimestamp, MonteCarloTimestamp;
		String buyOrSell;
		Integer transactionNo;
		private String reason;

		public TransactionRecord(Asset asset, String buyOrSell, float cost, float transactionPrice, float quantity,
				LocalDate transactionDate, double RGL, double rate, double revenue, LocalDate selectDate,
				float selectClose, Integer transactionNo, String reason, Timestamp batchTimestamp,
				Timestamp MonteCarloTimestamp) {
			this.buyOrSell = buyOrSell;
			if (this.buyOrSell.equals("B")) {
				this.buyDate = transactionDate;
				this.buyPrice = transactionPrice;
				this.buyQuantity = quantity;
			} else if (this.buyOrSell.equals("S")) {
				this.sellDate = transactionDate;
				this.buyDate = asset.getBuyDate();
				this.sellPrice = transactionPrice;
				this.buyPrice = asset.getBuyPrice();
				this.buyQuantity = asset.quantity;
				this.sellQuantity = quantity;
				this.revenue = asset.getRevenue();
				this.RGLRate = rate;
				this.RGL = RGL;
			} else {
				System.out.println("Please indicate this is a buy or sell.");
			}
			this.cost = cost;
			this.selectDate = selectDate;
			this.selectClose = selectClose;
			this.batchTimestamp = batchTimestamp;
			this.MonteCarloTimestamp = MonteCarloTimestamp;
			this.transactionNo = transactionNo;
			this.asset = asset;
			this.reason = reason;
		}
	}

	class AssetBuyingPlan {
		LocalDate pickDate;
		java.util.Date nextPickDate, lastPickDate;
		List<java.util.Date> pickDates;
		LocalDate vintageDate;
		LocalDate lowDate;
		public List<Map<String, Float>> listOfAssets;
	}

	Portfolio(java.sql.Date portfolioInitialDate, String style, double stylingThreshold, Connection conn,
			Statement stmt) {
		setAllAssetListPricesMapping(portfolioInitialDate, style, stylingThreshold, conn, stmt);
	}

	Portfolio(double initialCashInvestment, java.sql.Date portfolioInitialDate, String style, double stylingThreshold,
			Connection conn, Statement stmt) {
		setAllAssetListPricesMapping(portfolioInitialDate, style, stylingThreshold, conn, stmt);
		Portfolio.conn = conn;
		Portfolio.stmt = stmt;
		Portfolio.initialCashInvestment = initialCashInvestment;
		cashBalance = initialCashInvestment;
		assetList = new LinkedList<Asset>();
		statementList = new LinkedList<Asset>();
		totalAssetMarketValue = initialCashInvestment;
		initialTotalAsset = totalAssetMarketValue;
		this.portfolioInitialDate = portfolioInitialDate;
	}

	public Portfolio() {
		// TODO Auto-generated constructor stub
	}

	static Map<Date, Map<String, Map<Date, PriceNDesc[]>>> setAllAssetListPricesMapping(Date start, String style,
			double stylingThreshold, Connection conn, Statement stmt) {
		String select_all_assets;
		if (style.equals("ALL") || style == null) {
			select_all_assets = "select * from (select * from FJQ_select_stock where select_date>='" + start + "') f "
					+ "join " + "(select * from daily_snapshot where 交易日期 >='" + start
					+ "' and (is_index='N' or is_index is null)) d " + "on f.stock_code=substring(d.股票代码,3); ";
		} else if (style.equals("SM")) {
			select_all_assets = "select * from (select * from FJQ_select_stock where select_date>='" + start + "') f "
					+ "join " + "(select * from daily_snapshot where 交易日期 >='" + start + "' and 总市值 < "
					+ stylingThreshold * 2 + " and (is_index='N' or is_index is null)) d "
					+ "on f.stock_code=substring(d.股票代码,3); ";
		} else if (style.equals("L")) {
			select_all_assets = "select * from (select * from FJQ_select_stock where select_date>='" + start + "') f "
					+ "join " + "(select * from daily_snapshot where 交易日期 >='" + start + "' and 总市值 >= "
					+ stylingThreshold * 0.5 + " and (is_index='N' or is_index is null)) d "
					+ "on f.stock_code=substring(d.股票代码,3); ";
		} else {
			select_all_assets = "select * from (select * from FJQ_select_stock where select_date>='" + start + "') f "
					+ "join " + "(select * from daily_snapshot where 交易日期 >='" + start
					+ "' and (is_index='N' or is_index is null)) d " + "on f.stock_code=substring(d.股票代码,3); ";
		}
		System.out.println("select_all_assets: " + select_all_assets);
		Date selectDate, tradeDate;
		ResultSet rs;
		String stockCode;
		boolean rising, falling;
		float openPrice, closePrice, highPrice, lowPrice, MA_5, MA_10, MA_20, MA_30, MA_60;
		PriceNDesc[] priceAndDesc;
		double totalMarketValue, changeRate;
		// double liquidMarketValue;
		Map<String, Map<Date, PriceNDesc[]>> selectDateStockMap;
		Map<Date, PriceNDesc[]> assetPricesMap = null;
		try {
			rs = stmt.executeQuery(select_all_assets);
			while (rs.next()) {
				priceAndDesc = new PriceNDesc[9];
				selectDate = rs.getDate("select_date");
				tradeDate = rs.getDate("交易日期");
				stockCode = rs.getString("股票代码").substring(2);
				openPrice = rs.getFloat("开盘价");
				closePrice = rs.getFloat("收盘价");
				highPrice = rs.getFloat("最高价");
				lowPrice = rs.getFloat("最低价");
				MA_5 = rs.getFloat("MA_5");
				MA_10 = rs.getFloat("MA_10");
				MA_20 = rs.getFloat("MA_20");
				MA_30 = rs.getFloat("MA_30");
				MA_60 = rs.getFloat("MA_60");
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
				priceAndDesc[4] = new PriceNDesc(MA_5, rising, falling, totalMarketValue);
				priceAndDesc[5] = new PriceNDesc(MA_10, rising, falling, totalMarketValue);
				priceAndDesc[6] = new PriceNDesc(MA_20, rising, falling, totalMarketValue);
				priceAndDesc[7] = new PriceNDesc(MA_30, rising, falling, totalMarketValue);
				priceAndDesc[8] = new PriceNDesc(MA_60, rising, falling, totalMarketValue);
				if (allAssetListPricesMapping == null) {
					allAssetListPricesMapping = new HashMap<Date, Map<String, Map<Date, PriceNDesc[]>>>();
				}

				if (allAssetListPricesMapping.containsKey(selectDate)) {
					selectDateStockMap = allAssetListPricesMapping.get(selectDate);
					if (selectDateStockMap.containsKey(stockCode)) {
						assetPricesMap = selectDateStockMap.get(stockCode);
						assetPricesMap.putIfAbsent(tradeDate, priceAndDesc);
					} else {
						assetPricesMap = new HashMap<Date, PriceNDesc[]>();
						assetPricesMap.put(tradeDate, priceAndDesc);
					}
					selectDateStockMap.putIfAbsent(stockCode, assetPricesMap);
				} else {
					selectDateStockMap = new HashMap<String, Map<Date, PriceNDesc[]>>();
					assetPricesMap = new HashMap<Date, PriceNDesc[]>();
					assetPricesMap.put(tradeDate, priceAndDesc);
					selectDateStockMap.put(stockCode, assetPricesMap);
					allAssetListPricesMapping.put(selectDate, selectDateStockMap);
				}

				if (allAssetListFeatureMapping == null) {
					allAssetListFeatureMapping = new HashMap<String, Double>();
				}
				allAssetListFeatureMapping.putIfAbsent(stockCode, Double.valueOf(totalMarketValue));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return allAssetListPricesMapping;
	}

	void printAssets() {
		if (assetList != null) {
			System.out.println("Portfolio contains following " + assetList.size() + " Assets: ");
			for (Asset i : assetList)
				System.out.println(i.quantity + " share(s) of " + i.stockCode);
			System.out.println("... and " + cashBalance + " in cash.");
		} else {
			System.out.println("no asset avail, while " + cashBalance + " in cash.");
		}
	}

	static boolean getAssetRising(String stockCode, LocalDate selectDate, LocalDate priceDate, String risingOrFalling) {
		System.out.println("allAssetListPricesMapping priceDate " + priceDate);
		java.sql.Date selectDateSql = java.sql.Date.valueOf(selectDate);
		java.sql.Date priceDateSql = java.sql.Date.valueOf(priceDate);
		PriceNDesc assetPrice;
		try {
			assetPrice = allAssetListPricesMapping.get(selectDateSql).get(stockCode).get(priceDateSql)[0];
			System.out.println("select date: " + selectDateSql + ", stock: " + stockCode + ", price date: "
					+ priceDateSql + ", return: OPEN price: " + assetPrice.getPrice());
			if (risingOrFalling.equals("RISING"))
				return assetPrice.isOther();
			else if (risingOrFalling.equals("FALLING"))
				return assetPrice.isOther2();
			else
				return assetPrice.isOther();
		} catch (NullPointerException ne) {
			System.out.println("selectDate: " + selectDateSql + ", " + stockCode + " on " + priceDate
					+ ", in allAssetListPricesMapping.get(selectDateSql).get(stockCode).get(priceDateSql) is null");
			return false;
		}
	}

	int[] getAssetHistory(Asset a, LocalDate priceDate, int length) {
		int risingCount = 0, fallingCount = 0, len1, len2;
		int ind;
		len1 = len2 = length;
		ind = simulationDateRange.indexOf(priceDate);

		System.out.println("simulationDateRange: " + simulationDateRange);
		System.out.println("getAssetHistory d: " + priceDate);
		System.out.println("ind: " + ind);
		try {
			while (simulationDateRange.get(ind) != null) {
				if (getAssetRising(a.stockCode, a.getSelectDate(), simulationDateRange.get(ind), "RISING")) {
					risingCount++;
					System.out.println("on " + simulationDateRange.get(ind) + ", " + a.stockCode
							+ "'s rising history updated to " + risingCount);
				}
				ind--;
				if (ind < 0) {
					break;
				}
				if (len1 <= 1) {
					break;
				}
				len1--;
			}
			ind = simulationDateRange.indexOf(priceDate);
			while (simulationDateRange.get(ind) != null) {
				if (getAssetRising(a.stockCode, a.getSelectDate(), simulationDateRange.get(ind), "FALLING")) {
					fallingCount++;
					System.out.println("on " + simulationDateRange.get(ind) + ", " + a.stockCode
							+ "'s falling history updated to " + fallingCount);
				}
				ind--;
				if (ind < 0) {
					break;
				}
				if (len2 <= 1) {
					break;
				}
				len2--;
			}
		} catch (IndexOutOfBoundsException e) {
			e.printStackTrace();
		}
		return new int[] { risingCount, fallingCount };
	}

	void addAsset(Asset asset) {
		if (assetList == null) {
			List<Asset> assetList = new LinkedList<Asset>();
			this.assetList = assetList;
		} else {
			ListIterator<Asset> iterator = assetList.listIterator();
			iterator.add(asset);
		}
	}

	void addBulkAssets(List<Asset> al) {
		assetList.addAll(al);
		statementList.addAll(al);
	}

	void removeAsset(ListIterator<Asset> listIteartor) {
		listIteartor.remove();
	}

	void removeBulkAssets(List<Asset> al) {
		assetList.removeAll(al);
		statementList.removeAll(al);
	}

	static float getAssetPrice(String stockCode, LocalDate selectDate, LocalDate priceDate, String priceType) {
		System.out.println("allAssetListPricesMapping priceDate " + priceDate);
		java.sql.Date selectDateSql = java.sql.Date.valueOf(selectDate);
		java.sql.Date priceDateSql = java.sql.Date.valueOf(priceDate);
		PriceNDesc assetPrice;
		try {
			if (priceType.equals("OPEN")) {
				assetPrice = allAssetListPricesMapping.get(selectDateSql).get(stockCode).get(priceDateSql)[0];
				System.out.println("select date: " + selectDateSql + ", stock: " + stockCode + ", price date: "
						+ priceDateSql + ", return: OPEN price: " + assetPrice.getPrice());
				return assetPrice.getPrice();
			} else if (priceType.equals("CLOSE")) {
				assetPrice = allAssetListPricesMapping.get(selectDateSql).get(stockCode).get(priceDateSql)[1];
				System.out.println("select date: " + selectDateSql + ", stock: " + stockCode + ", price date: "
						+ priceDateSql + ", return: CLOSE price: " + assetPrice.getPrice());
				return assetPrice.getPrice();
			} else if (priceType.equals("HIGH")) {
				assetPrice = allAssetListPricesMapping.get(selectDateSql).get(stockCode).get(priceDateSql)[2];
				System.out.println("select date: " + selectDateSql + ", stock: " + stockCode + ", price date: "
						+ priceDateSql + ", return: HIGH price: " + assetPrice.getPrice());
				return assetPrice.getPrice();
			} else if (priceType.equals("LOW")) {
				assetPrice = allAssetListPricesMapping.get(selectDateSql).get(stockCode).get(priceDateSql)[3];
				System.out.println("select date: " + selectDateSql + " stock: " + stockCode + "price date: "
						+ priceDateSql + " return: LOW price: " + assetPrice.getPrice());
				return assetPrice.getPrice();
			} else if (priceType.equals("MA_5")) {
				assetPrice = allAssetListPricesMapping.get(selectDateSql).get(stockCode).get(priceDateSql)[4];
				System.out.println("select date: " + selectDateSql + " stock: " + stockCode + "price date: "
						+ priceDateSql + " return: LOW price: " + assetPrice.getPrice());
				return assetPrice.getPrice();
			} else if (priceType.equals("MA_10")) {
				assetPrice = allAssetListPricesMapping.get(selectDateSql).get(stockCode).get(priceDateSql)[5];
				System.out.println("select date: " + selectDateSql + " stock: " + stockCode + "price date: "
						+ priceDateSql + " return: LOW price: " + assetPrice.getPrice());
				return assetPrice.getPrice();
			} else if (priceType.equals("MA_20")) {
				assetPrice = allAssetListPricesMapping.get(selectDateSql).get(stockCode).get(priceDateSql)[6];
				System.out.println("select date: " + selectDateSql + " stock: " + stockCode + "price date: "
						+ priceDateSql + " return: LOW price: " + assetPrice.getPrice());
				return assetPrice.getPrice();
			} else if (priceType.equals("MA_30")) {
				assetPrice = allAssetListPricesMapping.get(selectDateSql).get(stockCode).get(priceDateSql)[7];
				System.out.println("select date: " + selectDateSql + " stock: " + stockCode + "price date: "
						+ priceDateSql + " return: LOW price: " + assetPrice.getPrice());
				return assetPrice.getPrice();
			} else if (priceType.equals("MA_60")) {
				assetPrice = allAssetListPricesMapping.get(selectDateSql).get(stockCode).get(priceDateSql)[8];
				System.out.println("select date: " + selectDateSql + " stock: " + stockCode + "price date: "
						+ priceDateSql + " return: LOW price: " + assetPrice.getPrice());
				return assetPrice.getPrice();
			} else {
				assetPrice = allAssetListPricesMapping.get(selectDateSql).get(stockCode).get(priceDateSql)[0];
				System.out.println("select date: " + selectDateSql + " stock: " + stockCode + "price date: "
						+ priceDateSql + " return: OPEN price: " + assetPrice.getPrice());
				return assetPrice.getPrice();
			}
		} catch (NullPointerException ne) {
			System.out.println("selectDate: " + selectDateSql + ", " + stockCode + " on " + priceDate
					+ ", in allAssetListPricesMapping.get(selectDateSql).get(stockCode).get(priceDateSql) is null");
			// return new Asset(stockCode).getAssetMarketValue(priceDate, "CLOSE", conn,
			// stmt);
			return new Asset(stockCode).getPrice(priceDate, priceType, conn, stmt).getActualPrice();
		}
	}

	static double getAssetScale(String stockCode, LocalDate selectDate, LocalDate priceDate, String atOpenOrClose) {
		System.out.println("allAssetListPricesMapping priceDate " + priceDate);
		java.sql.Date selectDateSql = java.sql.Date.valueOf(selectDate);
		java.sql.Date priceDateSql = java.sql.Date.valueOf(priceDate);
		PriceNDesc assetPrice;
		try {
			
				assetPrice = allAssetListPricesMapping.get(selectDateSql).get(stockCode).get(priceDateSql)[0];
				System.out.println("select date: " + selectDateSql + ", stock: " + stockCode + ", price date: "
						+ priceDateSql + ", return: OPEN price: " + assetPrice.getPrice());
				return assetPrice.getScale();
		} catch (NullPointerException ne) {
			System.out.println("selectDate: " + selectDateSql + ", " + stockCode + " on " + priceDate
					+ ", in allAssetListPricesMapping.get(selectDateSql).get(stockCode).get(priceDateSql) is null");
			// return new Asset(stockCode).getAssetMarketValue(priceDate, "CLOSE", conn,
			// stmt);
			return new Asset(stockCode).getPrice(priceDate, atOpenOrClose, conn, stmt).getActualScale();
		}
	}
	
	
	
	
	/*
	 * Map<String, float[]> getAssetListPricesMapping(LocalDate pricingDate,
	 * Connection conn, Statement stmt) { Map<String, float[]> assetPricesMap = new
	 * HashMap<String, float[]>(); if (assetList != null) { ListIterator<Asset> li =
	 * assetList.listIterator(); String buffer; String assetListString = ""; if
	 * (!li.hasNext()) { return null; } while (li.hasNext()) { buffer =
	 * li.next().stockCode; if (li.hasNext()) assetListString = assetListString +
	 * "股票代码 like '%" + buffer + "' or "; else assetListString = assetListString +
	 * "股票代码 like '%" + buffer + "'"; } String assetListPricingSQL =
	 * "select * from daily_snapshot where (" + assetListString + " ) and 交易日期 ='" +
	 * pricingDate + "' and (is_index is null or is_index='N');"; try { ResultSet rs
	 * = stmt.executeQuery(assetListPricingSQL); while (rs.next()) { String
	 * stockCode = rs.getString("股票代码"); openPrice = rs.getFloat("开盘价"); closePrice
	 * = rs.getFloat("收盘价"); highPrice = rs.getFloat("最高价"); lowPrice =
	 * rs.getFloat("最低价"); float[] prices = new float[4]; prices[0] = openPrice;
	 * prices[1] = closePrice; prices[2] = highPrice; prices[3] = lowPrice;
	 * assetPricesMap.put(stockCode.substring(2), prices); } } catch (SQLException
	 * e) { // TODO Auto-generated catch block e.printStackTrace(); } return
	 * assetPricesMap; } else { return assetPricesMap; } }
	 */

	double portfolioSecuritiesMarketValue(LocalDate pricingDate, String openOrClose, Connection conn, Statement stmt) {
		securitiesMarketValue = 0;
		if (assetList != null) {
			for (Asset asset : assetList) {
				// close price factored in.
				securitiesMarketValue = securitiesMarketValue + (double) asset.quantity
						* (double) getAssetPrice(asset.stockCode, asset.selectDate, pricingDate, openOrClose);
			}
			return securitiesMarketValue;
		} else {
			return securitiesMarketValue;
		}
	}

	double portfolioSecuritiesMarketValue(LocalDate pricingDate) {
		securitiesMarketValue = 0;
		for (Asset asset : assetList) {
			securitiesMarketValue = securitiesMarketValue + (double) asset.getAssetMarketValue(pricingDate, conn, stmt);
		}
		return securitiesMarketValue;
	}

	double portfolioCashBalance(LocalDate pricingDate) {
		return cashBalance;
	}

	double portfolioRealizedGainLoss() {
		realizedGainLoss = cashBalance - initialCashInvestment;
		return realizedGainLoss;
	}

	double portfolioChangeRate(LocalDate inquiryDate, String openOrClose) {
		consolidatedChange = 0;
		System.out.println("Portfolio Change Rate on " + inquiryDate + ": "
				+ portfolioTotalAsset(inquiryDate, openOrClose) / initialTotalAsset);
		return (portfolioTotalAsset(inquiryDate, openOrClose) / initialTotalAsset);
	}

	double portfolioUnrealizedGainLoss(LocalDate pricingDate, Connection conn, Statement stmt) {
		unrealizedGainLoss = 0;
		for (Asset asset : assetList) {
			// unrealizedGainLoss = unrealizedGainLoss +
			// asset.getAssetUnrealizedGainLoss(pricingDate, conn, stmt);
			unrealizedGainLoss = unrealizedGainLoss
					+ getAssetPrice(asset.stockCode, asset.selectDate, pricingDate, "CLOSE");
		}
		return unrealizedGainLoss;
	}

	synchronized boolean hibernatePositions(List<Asset> Assets, LocalDate date, boolean sellOffIndicator,
			Timestamp batchTimeStamp, Timestamp MonteCarloTimestamp, Connection conn, Statement stmt) {
		try {
			System.out.println("pricingDate: " + date);
			ZonedDateTime zonedTradeDate = ZonedDateTime.of(date, LocalTime.of(0, 0, 0), ZoneId.of("Asia/Shanghai"));
			String trade_date = zonedTradeDate.toString().split("T")[0];
			System.out.println("trade date: " + trade_date);
			float assetPrice;
			if (Assets != null) {
				for (Asset asset : Assets) {
					zonedTradeDate = ZonedDateTime.of(asset.buyDate, LocalTime.of(0, 0, 0), ZoneId.of("Asia/Shanghai"));
					String buy_date = zonedTradeDate.toString().split("T")[0];
					// assetPrice = assetPricesMap.get(asset.stockCode)[1];
					assetPrice = getAssetPrice(asset.stockCode, asset.selectDate, date, "CLOSE");
					String hibernate_positions_sql = " insert into hibernate_positions (asset, position, buy_date, price, market_value, cost, UGL_rate, trade_date, batch_timestamp, MonteCarlo_timestamp)"
							+ " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
					PreparedStatement preparedStmt = conn.prepareStatement(hibernate_positions_sql);
					preparedStmt.setString(1, asset.stockCode);
					preparedStmt.setFloat(2, asset.quantity);
					preparedStmt.setDate(3, Date.valueOf(buy_date));
					preparedStmt.setFloat(4, assetPrice);
					preparedStmt.setDouble(5, (double) (asset.quantity * assetPrice));
					preparedStmt.setDouble(6, asset.cost);
					preparedStmt.setDouble(7, (asset.quantity * assetPrice - asset.cost) / asset.cost);
					preparedStmt.setDate(8, Date.valueOf(trade_date));
					preparedStmt.setTimestamp(9, batchTimeStamp);
					preparedStmt.setTimestamp(10, MonteCarloTimestamp);
					System.out.println(preparedStmt.toString());
					preparedStmt.execute();
				}
				String hibernate_cash_sql = " insert into hibernate_positions (asset, position, buy_date, price, market_value, cost, UGL_rate, trade_date, batch_timestamp, MonteCarlo_timestamp)"
						+ " values (?, ?, ?, ?, ?,?,?,?,?,?)";
				PreparedStatement preparedStmt = conn.prepareStatement(hibernate_cash_sql);
				preparedStmt.setString(1, "CASH");
				preparedStmt.setFloat(2, -1);
				preparedStmt.setDate(3, Date.valueOf("1999-01-01"));
				preparedStmt.setFloat(4, -1);
				preparedStmt.setDouble(5, this.cashBalance);
				preparedStmt.setFloat(6, -1);
				preparedStmt.setFloat(7, -1);
				preparedStmt.setDate(8, Date.valueOf(trade_date));
				preparedStmt.setTimestamp(9, batchTimeStamp);
				preparedStmt.setTimestamp(10, MonteCarloTimestamp);
				System.out.println(preparedStmt.toString());
				preparedStmt.execute();

				String hibernate_securitiesMarketValue_sql = " insert into hibernate_positions (asset, position, buy_date, price, market_value, cost, UGL_rate, trade_date, batch_timestamp, MonteCarlo_timestamp)"
						+ " values (?, ?, ?, ?, ?,?,?,?,?,?)";
				preparedStmt = conn.prepareStatement(hibernate_securitiesMarketValue_sql);
				preparedStmt.setString(1, "securitiesMarketValue");
				preparedStmt.setFloat(2, -1);
				preparedStmt.setDate(3, Date.valueOf("1999-01-01"));
				preparedStmt.setFloat(4, -1);
				preparedStmt.setDouble(5, this.securitiesMarketValue);
				preparedStmt.setFloat(6, -1);
				preparedStmt.setFloat(7, -1);
				preparedStmt.setDate(8, Date.valueOf(trade_date));
				preparedStmt.setTimestamp(9, batchTimeStamp);
				preparedStmt.setTimestamp(10, MonteCarloTimestamp);
				System.out.println(preparedStmt.toString());
				preparedStmt.execute();

				String hibernate_totalAssetMarketValue_sql = " insert into hibernate_positions (asset, position, buy_date, price, market_value, cost, UGL_rate, trade_date, batch_timestamp, MonteCarlo_timestamp)"
						+ " values (?, ?, ?, ?, ?,?,?,?,?,?)";
				preparedStmt = conn.prepareStatement(hibernate_totalAssetMarketValue_sql);
				preparedStmt.setString(1, "totalAssetMarketValue");
				preparedStmt.setFloat(2, -1);
				preparedStmt.setDate(3, Date.valueOf("1999-01-01"));
				preparedStmt.setFloat(4, -1);
				preparedStmt.setDouble(5, this.totalAssetMarketValue);
				preparedStmt.setFloat(6, -1);
				preparedStmt.setFloat(7, -1);
				preparedStmt.setDate(8, Date.valueOf(trade_date));
				preparedStmt.setTimestamp(9, batchTimeStamp);
				preparedStmt.setTimestamp(10, MonteCarloTimestamp);
				System.out.println(preparedStmt.toString());
				preparedStmt.execute();
			}
		} catch (SQLException se) {
			// Handle errors for JDBC
			se.printStackTrace();
			return false;
		} catch (Exception e) {
			// Handle errors for Class.forName
			e.printStackTrace();
			return false;
		} finally {
		}
		return true;
	}

	synchronized boolean hibernateTransactions(TransactionRecord tr, LocalDate date, Connection conn, Statement stmt) {
		int holdingDays;
		LocalDate sellDate;
		try {
			System.out.println("pricingDate: " + date);
			ZonedDateTime zonedTradeDate = ZonedDateTime.of(date, LocalTime.of(0, 0, 0), ZoneId.of("Asia/Shanghai"));
			String trade_date = zonedTradeDate.toString().split("T")[0];
			System.out.println("trade date: " + trade_date);
			if (tr != null) {
				if (tr.buyOrSell.equals("B")) {
					zonedTradeDate = ZonedDateTime.of(tr.buyDate, LocalTime.of(0, 0, 0), ZoneId.of("Asia/Shanghai"));
					String buy_date = zonedTradeDate.toString().split("T")[0];
					String hibernate_transactions_sql = "insert into hibernate_transactions (transaction_No, transaction_type, stock_code, buy_date, "
							+ "buy_quantity, buy_price, cost, select_date, select_close, reason, batch_timestamp, MonteCarlo_timestamp)"
							+ " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
					PreparedStatement preparedStmt = conn.prepareStatement(hibernate_transactions_sql);
					preparedStmt.setInt(1, tr.transactionNo);
					preparedStmt.setString(2, tr.buyOrSell);
					preparedStmt.setString(3, tr.asset.stockCode);
					preparedStmt.setDate(4, Date.valueOf(buy_date));
					preparedStmt.setFloat(5, tr.buyQuantity);
					preparedStmt.setFloat(6, tr.buyPrice);
					preparedStmt.setFloat(7, tr.cost);
					preparedStmt.setDate(8, Date.valueOf(tr.selectDate));
					preparedStmt.setFloat(9, tr.selectClose);
					preparedStmt.setString(10, tr.reason);
					preparedStmt.setTimestamp(11, tr.batchTimestamp);
					preparedStmt.setTimestamp(12, tr.MonteCarloTimestamp);
					System.out.println(preparedStmt.toString());
					preparedStmt.execute();
				} else if (tr.buyOrSell.equals("S")) {
					zonedTradeDate = ZonedDateTime.of(date, LocalTime.of(0, 0, 0), ZoneId.of("Asia/Shanghai"));
					String sell_date = zonedTradeDate.toString().split("T")[0];
					sellDate = LocalDate.of(Integer.parseInt(sell_date.toString().split("-")[0]),
							Integer.parseInt(sell_date.toString().split("-")[1]),
							Integer.parseInt(sell_date.toString().split("-")[2]));
					holdingDays = simulationDateRange.indexOf((sellDate))
							- simulationDateRange.indexOf(tr.asset.getBuyDate()) + 1;
					tr.asset.setHoldingDays(holdingDays);
					String hibernate_transactions_sql = "insert into hibernate_transactions (transaction_No, transaction_type, stock_code,"
							+ " buy_date, sell_date, holding_days, buy_quantity, "
							+ "sell_quantity, buy_price, sell_price, cost, revenue, select_date, select_close, RGL, "
							+ "RGL_rate, gain_speed, reason, batch_timestamp, MonteCarlo_timestamp)"
							+ " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
					PreparedStatement preparedStmt = conn.prepareStatement(hibernate_transactions_sql);
					preparedStmt.setInt(1, tr.transactionNo);
					preparedStmt.setString(2, tr.buyOrSell);
					preparedStmt.setString(3, tr.asset.stockCode);
					preparedStmt.setDate(4, Date.valueOf(tr.asset.getBuyDate()));
					preparedStmt.setDate(5, Date.valueOf(sell_date));
					preparedStmt.setInt(6, holdingDays);
					preparedStmt.setFloat(7, tr.buyQuantity);
					preparedStmt.setFloat(8, tr.sellQuantity);
					preparedStmt.setFloat(9, tr.buyPrice);
					preparedStmt.setFloat(10, tr.sellPrice);
					preparedStmt.setFloat(11, tr.cost);
					preparedStmt.setDouble(12, tr.revenue);
					preparedStmt.setDate(13, Date.valueOf(tr.selectDate));
					preparedStmt.setFloat(14, tr.selectClose);
					preparedStmt.setDouble(15, tr.RGL);
					preparedStmt.setDouble(16, tr.RGLRate);
					preparedStmt.setFloat(17, tr.asset.getGainSpeed(sellDate, "CLOSE"));
					preparedStmt.setString(18, tr.reason);
					preparedStmt.setTimestamp(19, tr.batchTimestamp);
					preparedStmt.setTimestamp(20, tr.MonteCarloTimestamp);
					System.out.println(preparedStmt.toString());
					preparedStmt.execute();
				}
			}
		} catch (SQLException se) {
			// Handle errors for JDBC
			se.printStackTrace();
			return false;
		} catch (Exception e) {
			// Handle errors for Class.forName
			e.printStackTrace();
			return false;
		} finally {

		}
		return true;
	}

	void viewPortfolioGainLoss(LocalDate pricingDate) {
		System.out.println("portfolio realizedGainLoss: " + portfolioRealizedGainLoss());
		System.out.println("portfolio UnrealizedGainLoss: " + portfolioUnrealizedGainLoss(pricingDate, conn, stmt));
	}

	double portfolioTotalAsset(LocalDate pricingDate, String openOrClose) {
		totalAssetMarketValue = cashBalance + portfolioSecuritiesMarketValue(pricingDate, openOrClose, conn, stmt);
		return totalAssetMarketValue;
	}

	void accountingPortfolioBalances(LocalDate pricingDate, String openOrClose) {
		this.securitiesMarketValue = portfolioSecuritiesMarketValue(pricingDate, openOrClose, conn, stmt);
		this.totalAssetMarketValue = portfolioTotalAsset(pricingDate, openOrClose);
		System.out.println("On accounting day: " + pricingDate);
		System.out.println("Portfolio Securities Market Value: " + securitiesMarketValue + ", on: " + pricingDate);
		System.out.println("Portfolio Cash Balance: " + portfolioCashBalance(pricingDate) + ", on: " + pricingDate);
		System.out.println("Portfolio Total Asset value: " + totalAssetMarketValue + ", on: " + pricingDate);
		printAssets();
	}

	TransactionRecord buy(Asset asset, LocalDate buyDate, LocalDate selectDate, float selectClose, float quantity,
			String buyAtOpenOrClose, Timestamp batchTimeStamp, Timestamp MonteCarloTimestamp, double YINNChangeDaily) {
		System.out.println("portfolio before Buy:");
		printAssets();
		float buyPrice = getAssetPrice(asset.stockCode, asset.selectDate, buyDate, buyAtOpenOrClose);
		asset.setBuyPrice(buyPrice);
		asset.setBuyDate(buyDate);
		asset.setSelectDate(selectDate);
		asset.setSelectClose(selectClose);
		asset.setQuantity(quantity);
		cashBalance = cashBalance - asset.getCost();
		asset.bought = true;
		asset.inPortfolio = true;
		addAsset(asset);
		securitiesMarketValue = portfolioSecuritiesMarketValue(buyDate, buyAtOpenOrClose, conn, stmt);
		totalAssetMarketValue = cashBalance + securitiesMarketValue;
		System.out.println("portfolio after Buy:");
		printAssets();
		transactionNo++;
		TransactionRecord tr = new TransactionRecord(asset, "B", asset.getCost(), asset.getBuyPrice(), quantity,
				buyDate, 0.0, 0.0, 0.0, asset.getSelectDate(), asset.getSelectClose(), transactionNo, "BUY on "+buyDate+", YINN Change is "+YINNChangeDaily,
				batchTimeStamp, MonteCarloTimestamp);
		hibernateTransactions(tr, buyDate, conn, stmt);
		return tr;
	}

	TransactionRecord buy(Asset asset, LocalDate buyDate, LocalDate selectDate, float selectClose,
			String buyAtOpenOrClose, float quantity, Connection conn, Statement stmt, Timestamp batchTimeStamp,
			Timestamp MonteCarloTimestamp) {
		asset.setBuyPrice(getAssetPrice(asset.stockCode, selectDate, buyDate, buyAtOpenOrClose));
		asset.setBuyDate(buyDate);
		asset.setQuantity(quantity);
		System.out.println("asset " + asset.stockCode + ", bought on " + buyDate + " quantity set to " + quantity
				+ " buyPrice set to " + asset.buyPrice);
		cashBalance = cashBalance - asset.getCost();
		asset.bought = true;
		asset.inPortfolio = true;
		addAsset(asset);
		securitiesMarketValue = portfolioSecuritiesMarketValue(buyDate, buyAtOpenOrClose, conn, stmt);
		totalAssetMarketValue = cashBalance + securitiesMarketValue;
		System.out.println("portfolio after Buy:");
		transactionNo++;
		StockMarket.Portfolio.TransactionRecord tr = new TransactionRecord(asset, "B", asset.getCost(),
				asset.getBuyPrice(), asset.getQuantity(), buyDate, 0.0, 0.0, 0.0, asset.getSelectDate(), selectClose,
				transactionNo, "BUY", batchTimeStamp, MonteCarloTimestamp);
		hibernateTransactions(tr, buyDate, conn, stmt);
		return tr;
	}

	double attemptedBuy(Asset asset, LocalDate selectDate, LocalDate buyDate, float quantity, Connection conn,
			Statement stmt) {
		System.out.println("portfolio before attempted Buy:");
		printAssets();
		asset.setBuyPrice(getAssetPrice(asset.stockCode, selectDate, buyDate, "OPEN"));
		asset.setBuyDate(buyDate);
		asset.setQuantity(quantity);
		System.out.println("asset " + asset.stockCode + ", attempted-buy tested on " + buyDate);
		double attemptedCashBalance = cashBalance - asset.getCost();
		return attemptedCashBalance;
	}

	TransactionRecord sell(Asset asset, LocalDate sellDate, LocalDate selectDate, ListIterator<Asset> lIterator,
			String sellAtOpenOrClose, String reason, Timestamp batchTimeStamp, Timestamp MonteCarloTimestamp) {
		removeAsset(lIterator);
		float sellPrice = getAssetPrice(asset.stockCode, selectDate, sellDate, sellAtOpenOrClose);
		System.out.println(asset.stockCode + ", sell price: " + sellPrice + " on sell date: " + sellDate);
		asset.setSellPrice(sellPrice);
		asset.setSellDate(sellDate);
		asset.sold = true;
		asset.offPortfolio = true;
		System.out.println("cashBalance before sell: " + cashBalance);
		System.out.println("quantity: " + asset.quantity + ", sellPrice: " + asset.sellPrice);
		cashBalance = cashBalance + asset.getRevenue();
		System.out.println("quantity * sellPrice: " + asset.quantity * asset.sellPrice);
		System.out.println("cashBalance after sell: " + cashBalance);
		securitiesMarketValue = portfolioSecuritiesMarketValue(sellDate, sellAtOpenOrClose, conn, stmt);
		totalAssetMarketValue = cashBalance + securitiesMarketValue;
		System.out.println("portfolio after sell:");
		printAssets();
		transactionNo++;
		double rate = (asset.getRevenue() - asset.getCost()) / asset.getCost();
		TransactionRecord tr = new TransactionRecord(asset, "S", asset.getCost(), sellPrice, asset.getQuantity(),
				sellDate, asset.getRevenue() - asset.getCost(), rate, asset.getRevenue(), asset.getSelectDate(),
				asset.getSelectClose(), transactionNo, reason, batchTimeStamp, MonteCarloTimestamp);
		hibernateTransactions(tr, sellDate, conn, stmt);
		return tr;
	}

	TransactionRecord sell(Asset asset, LocalDate sellDate, float d, ListIterator<Asset> lIterator, String reason,
			Timestamp batchTimeStamp, Timestamp MonteCarloTimestamp) {
		removeAsset(lIterator);
		asset.setSellPrice(d);
		asset.setSellDate(sellDate);
		asset.sold = true;
		asset.offPortfolio = true;
		cashBalance = cashBalance + asset.getRevenue();
		System.out.println("quantity * sellPrice: " + asset.quantity * asset.sellPrice);
		System.out.println("cashBalance after sell: " + cashBalance);
		securitiesMarketValue = portfolioSecuritiesMarketValue(sellDate, "CLOSE", conn, stmt);
		totalAssetMarketValue = cashBalance + securitiesMarketValue;
		System.out.println("portfolio after sell:");
		printAssets();
		transactionNo++;
		double rate = (asset.getRevenue() - asset.getCost()) / asset.getCost();
		TransactionRecord tr = new TransactionRecord(asset, "S", asset.getCost(), d, asset.getQuantity(), sellDate,
				asset.getRevenue() - asset.getCost(), rate, asset.getRevenue(), asset.getSelectDate(),
				asset.getSelectClose(), transactionNo, reason, batchTimeStamp, MonteCarloTimestamp);
		hibernateTransactions(tr, sellDate, conn, stmt);
		return tr;
	}

	List<LocalDate> setSimulationDateRange(LocalDate start, LocalDate end) {
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

	AssetBuyingPlan accessABP(LocalDate pickDate, LocalDate simulationStart, String criteriaKey, int vintage, boolean scaled,
			Connection conn, Statement stmt) throws InterruptedException {
		Set<String> setOfAssets = new HashSet<String>();
		Set<Date> setOfPickDates = allAssetListPricesMapping.keySet();
		List<java.util.Date> listOfPickDates = new LinkedList<java.util.Date>();
		AssetBuyingPlan abp = new AssetBuyingPlan();
		abp.listOfAssets = new LinkedList<Map<String, Float>>();
		java.util.Date nextPickDate, lastPickDate;
		Iterator<Date> pi = setOfPickDates.iterator();
		Float selectDateClose = null;
		Double selectDateScale = null;
		Map<String, Float> CodenSelectDateClose;
		String stockCodeinGettingLowPrice = null;
		while (pi.hasNext()) {
			listOfPickDates.add(pi.next());
		}
		Collections.sort(listOfPickDates);
		Date pickDatesHead = (Date) listOfPickDates.get(0);
		System.out.println("in accessAssets: " + criteriaKey + " SQL : " + Criterion.getCretiria(criteriaKey)
				+ pickDate.getYear() + "-" + pickDate.getMonthValue() + "-" + pickDate.getDayOfMonth() + "';");
		java.sql.Date pickDateUtil = java.sql.Date.valueOf(pickDate);
		System.out.println("pickDate: " + pickDate);
		System.out.println("pickDateUtil before: " + pickDateUtil);
		while (Portfolio.allAssetListPricesMapping.get(pickDateUtil) == null && !pickDate.isBefore(simulationStart)) {
			System.out.println("pickDateUtil before: " + pickDateUtil);
			pickDate = pickDate.minusDays(1);
			System.out.println("pickDate -1: " + pickDate);
			pickDateUtil = java.sql.Date.valueOf(pickDate);
		}

		if (!pickDateUtil.after(Date.valueOf(simulationStart))) {
			pickDateUtil = pickDatesHead;
			pickDate = pickDateUtil.toLocalDate();
		}
		System.out.println("pickDateUtil after: " + pickDateUtil);
		try {
			System.out.println("on pickDate: " + pickDate + " Asset List: "
					+ allAssetListPricesMapping.get(pickDateUtil).keySet());
			for (String a : allAssetListPricesMapping.get(pickDateUtil).keySet()) {
				System.out.println(a);
			}
			setOfAssets = allAssetListPricesMapping.get(pickDateUtil).keySet();
			Iterator<String> si = setOfAssets.iterator();
			while (si.hasNext()) {
				try {
					stockCodeinGettingLowPrice = si.next();
					selectDateClose = Float
							.valueOf(getAssetPrice(stockCodeinGettingLowPrice, pickDate, pickDate, "CLOSE"));
					selectDateScale = Double
							.valueOf(getAssetScale(stockCodeinGettingLowPrice, pickDate, pickDate, "CLOSE"));
				} catch (java.lang.NullPointerException ne) {
					ne.printStackTrace();
				}
				CodenSelectDateClose = new HashMap<String, Float>();
				if (scaled) {
					if (selectDateScale<12000000000.0) {
					CodenSelectDateClose.put(stockCodeinGettingLowPrice, selectDateClose);}
					else {						
					}
				} else {
				CodenSelectDateClose.put(stockCodeinGettingLowPrice, selectDateClose);
				}
				abp.listOfAssets.add(CodenSelectDateClose);
			}
			int index = listOfPickDates.indexOf(pickDateUtil);
			if (index < listOfPickDates.size() - 1) {
				nextPickDate = listOfPickDates.get(index + 1);
			} else {
				nextPickDate = null;
			}

			if (index > 0) {
				lastPickDate = listOfPickDates.get(index - 1);
			} else {
				lastPickDate = listOfPickDates.get(index);
			}
			abp.pickDate = pickDate;
			abp.nextPickDate = nextPickDate;
			abp.lastPickDate = lastPickDate;
			abp.pickDates = listOfPickDates;
			int pickDateIndex = simulationDateRange.indexOf(abp.pickDate);
			try {
				if (pickDateIndex + 1 < simulationDateRange.size())
					abp.vintageDate = simulationDateRange.get(pickDateIndex + vintage);
				else
					abp.vintageDate = abp.pickDate;
				if (abp != null)
					vintageSchedule.put(abp.vintageDate, abp);
			} catch (IndexOutOfBoundsException e) {
				e.printStackTrace();
				System.out.println("abp.pickDate: " + abp.pickDate);
				System.out.println("pickDateIndex: " + pickDateIndex);
				// System.out.println("simulationDateRange.get(pickDateIndex + vintage):
				// "+simulationDateRange.get(pickDateIndex + vintage));
			}

		} catch (NullPointerException ne) {
			ne.printStackTrace();
			return null;
		}
		if (pickDate.isBefore(simulationStart)) {
			return null;
		} else {
			return abp;
		}
	}
	
	
	
	AssetBuyingPlan accessRandomStocks(LocalDate pickDate, LocalDate simulationStart, String criteriaKey, int vintage,
			Connection conn, Statement stmt) throws InterruptedException {
		Set<String> setOfAssets = new HashSet<String>();
		Set<Date> setOfPickDates = allAssetListPricesMapping.keySet();
		List<java.util.Date> listOfPickDates = new LinkedList<java.util.Date>();
		AssetBuyingPlan abp = new AssetBuyingPlan();
		abp.listOfAssets = new LinkedList<Map<String, Float>>();
		java.util.Date nextPickDate, lastPickDate;
		Iterator<Date> pi = setOfPickDates.iterator();
		Float selectDateClose = null;
		Map<String, Float> CodenSelectDateClose;
		String stockCodeinGettingLowPrice = null;
		while (pi.hasNext()) {
			listOfPickDates.add(pi.next());
		}
		Collections.sort(listOfPickDates);
		Date pickDatesHead = (Date) listOfPickDates.get(0);
		System.out.println("in accessAssets: " + criteriaKey + " SQL : " + Criterion.getCretiria(criteriaKey)
				+ pickDate.getYear() + "-" + pickDate.getMonthValue() + "-" + pickDate.getDayOfMonth() + "';");
		java.sql.Date pickDateUtil = java.sql.Date.valueOf(pickDate);
		System.out.println("pickDate: " + pickDate);
		System.out.println("pickDateUtil before: " + pickDateUtil);
		while (Portfolio.allAssetListPricesMapping.get(pickDateUtil) == null && !pickDate.isBefore(simulationStart)) {
			System.out.println("pickDateUtil before: " + pickDateUtil);
			pickDate = pickDate.minusDays(1);
			System.out.println("pickDate -1: " + pickDate);
			pickDateUtil = java.sql.Date.valueOf(pickDate);
		}

		if (!pickDateUtil.after(Date.valueOf(simulationStart))) {
			pickDateUtil = pickDatesHead;
			pickDate = pickDateUtil.toLocalDate();
		}
		System.out.println("pickDateUtil after: " + pickDateUtil);
		try {
			System.out.println("on pickDate: " + pickDate + " Asset List: "
					+ allAssetListPricesMapping.get(pickDateUtil).keySet());
			for (String a : allAssetListPricesMapping.get(pickDateUtil).keySet()) {
				System.out.println(a);
			}
			setOfAssets = allAssetListPricesMapping.get(pickDateUtil).keySet();
			Iterator<String> si = setOfAssets.iterator();
			while (si.hasNext()) {
				try {
					stockCodeinGettingLowPrice = si.next();
					selectDateClose = Float
							.valueOf(getAssetPrice(stockCodeinGettingLowPrice, pickDate, pickDate, "CLOSE"));
				} catch (java.lang.NullPointerException ne) {
					ne.printStackTrace();
				}
				CodenSelectDateClose = new HashMap<String, Float>();
				CodenSelectDateClose.put(stockCodeinGettingLowPrice, selectDateClose);
				abp.listOfAssets.add(CodenSelectDateClose);
			}
			int index = listOfPickDates.indexOf(pickDateUtil);
			if (index < listOfPickDates.size() - 1) {
				nextPickDate = listOfPickDates.get(index + 1);
			} else {
				nextPickDate = null;
			}

			if (index > 0) {
				lastPickDate = listOfPickDates.get(index - 1);
			} else {
				lastPickDate = listOfPickDates.get(index);
			}
			abp.pickDate = pickDate;
			abp.nextPickDate = nextPickDate;
			abp.lastPickDate = lastPickDate;
			abp.pickDates = listOfPickDates;
			int pickDateIndex = simulationDateRange.indexOf(abp.pickDate);
			try {
				if (pickDateIndex + 1 < simulationDateRange.size())
					abp.vintageDate = simulationDateRange.get(pickDateIndex + vintage);
				else
					abp.vintageDate = abp.pickDate;
				if (abp != null)
					vintageSchedule.put(abp.vintageDate, abp);
			} catch (IndexOutOfBoundsException e) {
				e.printStackTrace();
				System.out.println("abp.pickDate: " + abp.pickDate);
				System.out.println("pickDateIndex: " + pickDateIndex);
				// System.out.println("simulationDateRange.get(pickDateIndex + vintage):
				// "+simulationDateRange.get(pickDateIndex + vintage));
			}

		} catch (NullPointerException ne) {
			ne.printStackTrace();
			return null;
		}
		if (pickDate.isBefore(simulationStart)) {
			return null;
		} else {
			return abp;
		}
	}
	

	Vector<Map<String, java.util.Date>> accessAssetStack(LocalDate pickDate, LocalDate simulationStart,
			String criteriaKey, Connection conn, Statement stmt) throws InterruptedException {
		Vector<Map<String, java.util.Date>> assetStack;
		List<java.util.Date> pickDatesAhead;
		// Date pickDateHead;
		if (pickDate.isBefore(simulationStart)) {
			return null;
		} else {
			Set<Date> setOfPickDates = allAssetListPricesMapping.keySet();
			List<java.util.Date> listOfPickDates = new LinkedList<java.util.Date>();
			AssetBuyingPlan abp = new AssetBuyingPlan();
			abp.listOfAssets = new LinkedList<Map<String, Float>>();
			// java.util.Date nextPickDate, lastPickDate;
			Iterator<Date> pi = setOfPickDates.iterator();
			Map<java.util.Date, Map<String, Map<Date, PriceNDesc[]>>> assetMapAhead = null;
			while (pi.hasNext()) {
				listOfPickDates.add(pi.next());
			}
			Collections.sort(listOfPickDates);
			// pickDateHead = (Date) listOfPickDates.get(0);
			System.out.println("in accessAssets: " + criteriaKey + " SQL : " + Criterion.getCretiria(criteriaKey)
					+ pickDate.getYear() + "-" + pickDate.getMonthValue() + "-" + pickDate.getDayOfMonth() + "';");
			java.sql.Date pickDateUtil = java.sql.Date.valueOf(pickDate);
			System.out.println("pickDate: " + pickDate);
			pickDatesAhead = new LinkedList<java.util.Date>();
			for (java.util.Date d : listOfPickDates) {
				if (!pickDateUtil.before(d)) {
					pickDatesAhead.add(d);
				}
			}

			assetMapAhead = new HashMap<java.util.Date, Map<String, Map<Date, PriceNDesc[]>>>();
			for (java.util.Date d : pickDatesAhead) {
				System.out.println("d: " + d);
				System.out.println("allAssetListPricesMapping.get(d): " + allAssetListPricesMapping.get(d));

				assetMapAhead.put(d, allAssetListPricesMapping.get(d));
			}
			System.out.println("allAssetListPricesMapping keys:" + allAssetListPricesMapping.keySet());
			System.out.println("assetMapAhead keys:" + assetMapAhead.keySet());

			assetStack = new Stack<Map<String, java.util.Date>>();
			Map<String, Map<Date, PriceNDesc[]>> assetMapOnCertainPickDate;
			for (java.util.Date d : pickDatesAhead) {
				assetMapOnCertainPickDate = assetMapAhead.get(d);
				for (String s : assetMapOnCertainPickDate.keySet()) {
					Map<String, java.util.Date> codePriceListPair = new HashMap<String, java.util.Date>();
					codePriceListPair.put(s, d);
					((Stack<Map<String, java.util.Date>>) assetStack).push(codePriceListPair);
				}
			}
			return assetStack;
		}
	}

	void sellOff(LocalDate adjustmentHappeningDate, String openOrClose, Timestamp batchTimestamp,
			Timestamp MonteCarloTimestamp, Connection conn, Statement stmt) {
		ListIterator<Asset> positionIterator;
		Asset asset;
		if (this.assetList != null) {
			positionIterator = this.assetList.listIterator();
			while (positionIterator.hasNext()) {
				asset = positionIterator.next();
				sell(asset, adjustmentHappeningDate, asset.getSelectDate(), positionIterator, openOrClose,
						"SELL OFF ALL. On " + adjustmentHappeningDate, batchTimestamp, MonteCarloTimestamp);
			}
		}
	}

	void adjustment(List<Asset> assetsToBuyWithSelectDate, LocalDate adjustmentHappeningDate, LocalDate betterBuyingDay,
			boolean elastic, double positionScale, double sellOffThreshold, double stopLoss, double retainGain,
			double lowerBoundSpeed, double averageSpeed, String strategy, String intraDayStrategy, String sellingStrategy,
			int volatilityAcceptanceNumber, double faultTradeTolerance, boolean bullishPredicted, Timestamp batchTimestamp,
			Timestamp MonteCarloTimestamp, Connection conn, Statement stmt) {
		float ComputingQuantity;
		String reasoned = "";
		double eachPositionAmount = 0, elasticRetainGain, YINNChangeDaily = 0;
		float lastDayClose, lastLastDayClose, lastLastLastDayClose = 0, intraDayHigh, intraDayLow, dayOpen, dayClose, MA_5, MA_10, MA_20, MA_30, MA_60;
		float assetBuyPrice, assetSelectClose;
		Double assetScale;
		String scaleIndicator, assetCode = null;
		ListIterator<Asset> positionAssetIterator;
		LocalDate selectLocalDate;
		boolean sellOff = false;
		// int risingCount, fallingCount;
        LocalDate previousAdjustmentHappeningDate = null, pPAdjustmentHappeningDate = null, pPPAdjustmentHappeningDate = null;
        if (simulationDateRange.indexOf(adjustmentHappeningDate)-3 >=0) {
        	previousAdjustmentHappeningDate = simulationDateRange.get(simulationDateRange.indexOf(adjustmentHappeningDate)-1);
        	pPAdjustmentHappeningDate = simulationDateRange.get(simulationDateRange.indexOf(adjustmentHappeningDate)-2);
        	pPPAdjustmentHappeningDate = simulationDateRange.get(simulationDateRange.indexOf(adjustmentHappeningDate)-3);
        	}
        else if (simulationDateRange.indexOf(adjustmentHappeningDate)-2 >=0) {
        	previousAdjustmentHappeningDate=adjustmentHappeningDate;
        	pPAdjustmentHappeningDate = simulationDateRange.get(simulationDateRange.indexOf(adjustmentHappeningDate)-2);
        }
        else if (simulationDateRange.indexOf(adjustmentHappeningDate)-1 >=0) {
        	previousAdjustmentHappeningDate = simulationDateRange.get(simulationDateRange.indexOf(adjustmentHappeningDate)-1);
        }
        else {
        	previousAdjustmentHappeningDate = adjustmentHappeningDate;
        }
        	
        String YINN = "select * from YINN where date='"+adjustmentHappeningDate+"'";
        ResultSet rs;
		try {
			rs = stmt.executeQuery(YINN);
			while (rs.next()) {
				System.out.println(rs.getLong("date_long"));
				System.out.println(rs.getDate("date"));
				System.out.println(rs.getDouble("change_daily"));
				YINNChangeDaily=rs.getDouble("change_daily");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (this.assetList != null) {
			positionAssetIterator = this.assetList.listIterator();
		} else {
			List<Asset> dummy = new LinkedList<Asset>();
			positionAssetIterator = dummy.listIterator();
		}
		ListIterator<Asset> toBuyIterator = assetsToBuyWithSelectDate.listIterator();
		while (toBuyIterator.hasNext()) {
			System.out.println("toBuy assets: " + toBuyIterator.next());
		}
		toBuyIterator = assetsToBuyWithSelectDate.listIterator();

		if (sellOffThreshold > 0) {
			if (portfolioTotalAsset(adjustmentHappeningDate, "OPEN") > sellOffThreshold * initialTotalAsset) {
				sellOff(adjustmentHappeningDate, "OPEN", batchTimestamp, MonteCarloTimestamp, conn, stmt);
				initialTotalAsset = portfolioTotalAsset(adjustmentHappeningDate, "OPEN");
				initialCashInvestment = initialTotalAsset;
				sellOff = true;
			}
		}

		while (positionAssetIterator.hasNext()) {
			Asset asset = positionAssetIterator.next();
			reasoned = " ";
			System.out.println("Asset " + asset.stockCode + ", quantity: " + asset.quantity + ", this Asset cost: "
					+ asset.getCost());
			assetScale = allAssetListFeatureMapping.get(asset.stockCode);
			if (elastic) {
				if (assetScale > 10000000000.0)
					scaleIndicator = "L";
				else
					scaleIndicator = "SM";
				if (scaleIndicator.equals("L")) {
					elasticRetainGain = retainGain * 0.5;
					// stopLoss=stopLoss*0.5;
				} else {
					elasticRetainGain = retainGain;
				}
			} else
				elasticRetainGain = retainGain;
			lastDayClose= getAssetPrice(asset.stockCode, asset.selectDate, previousAdjustmentHappeningDate, "CLOSE");
			
			if (pPAdjustmentHappeningDate!=null)
				lastLastDayClose= getAssetPrice(asset.stockCode, asset.selectDate, pPAdjustmentHappeningDate, "CLOSE");
			else 
				lastLastDayClose=lastDayClose;

			if (pPPAdjustmentHappeningDate!=null)
				lastLastLastDayClose= getAssetPrice(asset.stockCode, asset.selectDate, pPPAdjustmentHappeningDate, "CLOSE");
			else if (pPPAdjustmentHappeningDate==null && pPAdjustmentHappeningDate!=null)
				lastLastLastDayClose=lastLastDayClose;
			else if (pPPAdjustmentHappeningDate==null && pPAdjustmentHappeningDate==null)
				lastLastLastDayClose=lastDayClose;
			
			
			intraDayHigh = getAssetPrice(asset.stockCode, asset.selectDate, adjustmentHappeningDate, "HIGH");
			intraDayLow = getAssetPrice(asset.stockCode, asset.selectDate, adjustmentHappeningDate, "LOW");
			dayClose = getAssetPrice(asset.stockCode, asset.selectDate, adjustmentHappeningDate, "CLOSE");
			dayOpen = getAssetPrice(asset.stockCode, asset.selectDate, adjustmentHappeningDate, "OPEN");
			MA_5 = getAssetPrice(asset.stockCode, asset.selectDate, adjustmentHappeningDate, "MA_5");
			MA_10 = getAssetPrice(asset.stockCode, asset.selectDate, adjustmentHappeningDate, "MA_10");
			MA_20 = getAssetPrice(asset.stockCode, asset.selectDate, adjustmentHappeningDate, "MA_20");
			MA_30 = getAssetPrice(asset.stockCode, asset.selectDate, adjustmentHappeningDate, "MA_30");
			MA_60 = getAssetPrice(asset.stockCode, asset.selectDate, adjustmentHappeningDate, "MA_60");
			
			float triPreviousAverageClose = (lastLastLastDayClose + lastLastDayClose + lastDayClose)/3;
			
			
			// adjustmentHappeningDate, "OPEN");
			if (dayClose <= 0) {
				if (asset.getPrice(adjustmentHappeningDate, "OPEN", conn, stmt).getActualPrice() <= 0) {
					System.out.println(asset.stockCode + " has NO price info on " + adjustmentHappeningDate
							+ ", so skipping selling this asset on this day...");
					continue;
				}
			} else {
				double assetUnrealizedChange = asset.getAssetUnrealizedChange(adjustmentHappeningDate, "OPEN", conn,
						stmt);
				double assetUnrealizedGL = asset.getAssetUnrealizedGainLoss(adjustmentHappeningDate, "OPEN", conn,
						stmt);
				asset.getHoldingDays(adjustmentHappeningDate, simulationDateRange);
				double assetGainSpeed = asset.getGainSpeed(adjustmentHappeningDate, "CLOSE");
				System.out.println("Asset Unrealized Change is: " + assetUnrealizedChange);
				System.out.println("Asset Gain Speed is: " + assetGainSpeed);
				System.out.println(
						"Asset holding days is: " + asset.getHoldingDays(adjustmentHappeningDate, simulationDateRange));
				if (asset.stockCode.startsWith("30") || asset.stockCode.startsWith("688")) {
					//if (intraDayStrategy.equals("HaulingAtHighLow") && (intraDayHigh >= asset.getBuyPrice() * UHaul.getHighBound1())) {
					if (intraDayStrategy.equals("HaulingAtHighLow") && (intraDayHigh >= triPreviousAverageClose * UHaul.getHighBound1())) {
						if (adjustmentHappeningDate.isAfter(asset.buyDate)) {
							sell(asset, adjustmentHappeningDate, (float) (triPreviousAverageClose * UHaul.getHighBound1()), positionAssetIterator,
									"30/688, intraday gain surpasses "+(UHaul.getHighBound1()-1)*100+"% of buy price: " + triPreviousAverageClose,
									batchTimestamp, MonteCarloTimestamp);
							System.out.println("asset " + asset.stockCode + " sold on: " + adjustmentHappeningDate
									+ " at " + triPreviousAverageClose * UHaul.getHighBound1()
									+ " due to its intraday gain surpasses "+(UHaul.getHighBound1()-1)*100+"% of buy price: " + triPreviousAverageClose
									+ " with intraday HIGH price " + intraDayHigh);
						}
					} 
//					else if (sellingStrategy.equals("MA") && dayClose > MA_20 && intraDayHigh >= asset.getBuyPrice() * UHaul.getHighBoundMA1()) {
//						if (adjustmentHappeningDate.isAfter(asset.buyDate)) {
//							sell(asset, adjustmentHappeningDate, (float) (asset.getBuyPrice() * UHaul.getHighBoundMA1()),
//									positionAssetIterator,
//									"30/688, above MA_10, intraday gain surpasses "+(UHaul.getHighBoundMA1()-1)*100+"% of buy price: " + asset.getBuyPrice(),
//									batchTimestamp, MonteCarloTimestamp);
//							System.out.println("asset " + asset.stockCode + " sold on: " + adjustmentHappeningDate
//									+ " at " + asset.getBuyPrice() * UHaul.getHighBoundMA1()
//									+ " above MA_10, due to its intraday gain surpasses "+(UHaul.getHighBoundMA1()-1)*100+"% of buy price: " + asset.getBuyPrice()
//									+ " with intraday HIGH price " + intraDayHigh);
//						}
//					}
					
					else if (sellingStrategy.equals("MA") && dayClose < MA_20) {
						System.out.println(
								"reason 5: assetUnrealizedChange <= stopLoss || assetUnrealizedChange * 1.5 > elasticRetainGain * 1.5");
						reasoned = "SELL Trigger, reason 5: 30/688, dayClose < MA_20";
					}
					else if (!sellingStrategy.equals("MA") && intraDayStrategy.equals("HaulingAtHighLow") && (intraDayLow <= triPreviousAverageClose * UHaul.getLowBound1()))
					 {
						if (adjustmentHappeningDate.isAfter(asset.buyDate)) {
							sell(asset, adjustmentHappeningDate, (float) (triPreviousAverageClose * UHaul.getLowBound1()), positionAssetIterator,
									//"30/688, intraday gain plummet "+(1-UHaul.getLowBound1())*100+"% of buy price: " + triPreviousAverageClose,
									"30/688, intraday gain plummet "+(1-UHaul.getLowBound1())*100+"% of triPreviousAverageClose price: " + triPreviousAverageClose,
									batchTimestamp, MonteCarloTimestamp);
							System.out.println("asset " + asset.stockCode + " sold on: " + adjustmentHappeningDate
									+ " at " + triPreviousAverageClose * UHaul.getLowBound1()
									+ " due to its intraday gain plummet "+(1-UHaul.getLowBound1())*100+"% of buy price: " +triPreviousAverageClose
									+ " with intraday LOW price " + intraDayLow);
						}
					} else if (!sellingStrategy.equals("MA") &&  assetUnrealizedChange <= stopLoss * 1.5
							|| assetUnrealizedChange > elasticRetainGain * 1.5) {
						System.out.println(
								"reason 0: assetUnrealizedChange <= stopLoss || assetUnrealizedChange * 1.5 > elasticRetainGain * 1.5");
						reasoned = "SELL Trigger, reason 0: 30/688, assetUnrealizedChange <= stopLoss * 1.5 || assetUnrealizedChange > elasticRetainGain* 1.5";
					}  else if (!sellingStrategy.equals("MA") && assetGainSpeed < lowerBoundSpeed * 1.5) {
						System.out.println("reason 2: 30/688, assetGainSpeed < lowerBoundSpeed * 1.5");
						reasoned = "SELL Trigger, reason 2: 30/688, assetGainSpeed < lowerBoundSpeed * 1.5";
					} else if (!sellingStrategy.equals("MA") && assetGainSpeed < -averageSpeed * 1.5
							&& asset.getHoldingDays(adjustmentHappeningDate, simulationDateRange) >= 5) {
						System.out.println("reason 3: 30/688, assetGainSpeed < averageSpeed * 1.5 && holdingDays >= 5");
						reasoned = "SELL Trigger, reason 3: 30/688, assetGainSpeed < averageSpeed * 1.5 && holdingDays >= 5";
					} 
//					else if (assetUnrealizedChange < 0 
//							//&& assetUnrealizedChange < elasticRetainGain * 1.5
//							&& asset.getHoldingDays(adjustmentHappeningDate, simulationDateRange) >= 10) {
//						System.out.println(
//								"reason 1: assetUnrealizedChange <= stopLoss && assetUnrealizedChange * 1.5 > elasticRetainGain * 1.5");
//						reasoned = "SELL Trigger, reason 1: 30/688, assetUnrealizedChange < 0 && HoldingDays<=10";
//					}
//					else if (assetGainSpeed > averageSpeed * 1.5
//							&& asset.getHoldingDays(adjustmentHappeningDate, simulationDateRange) >= 5) {
//						System.out.println(
//								"reason 3: 30/688, assetGainSpeed < averageSpeed * 1.5 && holdingDays >= 5");
//						reasoned = "SELL Trigger, reason 3: 30/688, assetGainSpeed < averageSpeed * 1.5 && holdingDays >= 5";
//					} 
					else if (!sellingStrategy.equals("MA") && assetUnrealizedChange < elasticRetainGain * 1.5
							&& asset.getHoldingDays(adjustmentHappeningDate, simulationDateRange) >= 20) {
						System.out.println(
								"reason 4: 30/688, assetUnrealizedChange < elasticRetainGain * 1.5 && holdingDays >=  >= 20");
						reasoned = "SELL Trigger, reason 4: 30/688, assetUnrealizedChange < elasticRetainGain * 1.5 && holdingDays >= 20";
					}
//						else if (assetUnrealizedGL < -faultTradeTolerance * 1.3333) {
//						System.out.println("reason 5: 30/688, assetUnrealizedGL < "+faultTradeTolerance * 1.3333);
//						reasoned = "SELL Trigger, reason 5: 30/688, assetUnrealizedGL < "+faultTradeTolerance * 1.3333;
//					}	

					if (adjustmentHappeningDate.isAfter(asset.buyDate) && reasoned.startsWith("SELL Trigger")) {
						sell(asset, adjustmentHappeningDate, asset.getSelectDate(), positionAssetIterator, "OPEN",
								reasoned, batchTimestamp, MonteCarloTimestamp);
						System.out.println("asset " + asset.stockCode + " sold on: " + adjustmentHappeningDate + " at "
								+ getAssetPrice(asset.stockCode, asset.selectDate, adjustmentHappeningDate, "OPEN")
								+ " due to " + reasoned);
					}
				} 
				
				else {
					//if (intraDayStrategy.equals("HaulingAtHighLow") && (intraDayHigh >= asset.getBuyPrice() * UHaul.getHighBound2())) {
					if (intraDayStrategy.equals("HaulingAtHighLow") && (intraDayHigh >= triPreviousAverageClose * UHaul.getHighBound2())) {
						if (adjustmentHappeningDate.isAfter(asset.buyDate)) {
							sell(asset, adjustmentHappeningDate, (float) (triPreviousAverageClose *  UHaul.getHighBound2()), positionAssetIterator,
									"HaulingAtHighLow && (intraDayHigh >= triPreviousAverageClose * "+UHaul.getHighBound2()+"))", batchTimestamp,
									MonteCarloTimestamp);
							System.out.println("asset " + asset.stockCode + " sold on: " + adjustmentHappeningDate
									+ " at " + triPreviousAverageClose *  UHaul.getHighBound2()
									+ " due to its intraday gain surpasses "+(UHaul.getHighBound2()-1)*100+"% of buy price: " + asset.getBuyPrice()
									+ " with intraday HIGH price " + intraDayHigh);
						}
					}
//					else if (sellingStrategy.equals("MA") && dayClose > MA_20 && intraDayHigh >= asset.getBuyPrice() * UHaul.getHighBoundMA2()) {
//						if (adjustmentHappeningDate.isAfter(asset.buyDate)) {
//							sell(asset, adjustmentHappeningDate, (float) (asset.getBuyPrice() * UHaul.getHighBoundMA2()),
//									positionAssetIterator,
//									"above MA_10, intraday gain surpasses "+(UHaul.getHighBoundMA2()-1)*100+"% of buy price: " + asset.getBuyPrice(),
//									batchTimestamp, MonteCarloTimestamp);
//							System.out.println("asset " + asset.stockCode + " sold on: " + adjustmentHappeningDate
//									+ " at " + asset.getBuyPrice() * UHaul.getHighBoundMA2()
//									+ " above MA_10, due to its intraday gain surpasses "+(UHaul.getHighBoundMA2()-1)*100+"% of buy price: " + asset.getBuyPrice()
//									+ " with intraday HIGH price " + intraDayHigh);
//						}
//					}
					else if (sellingStrategy.equals("MA") && dayClose < MA_20) {
						System.out.println(
								"reason 5: assetUnrealizedChange <= stopLoss || assetUnrealizedChange * 1.5 > elasticRetainGain * 1.5");
						reasoned = "SELL Trigger, reason 5: dayClose < MA_20";
					}
						
					else if (!sellingStrategy.equals("MA") && intraDayStrategy.equals("HaulingAtHighLow")
							//&& (intraDayLow <= asset.getBuyPrice() * UHaul.getLowBound2())) { 
							&& (intraDayLow <= triPreviousAverageClose * UHaul.getLowBound2())) {
						if (adjustmentHappeningDate.isAfter(asset.buyDate)) {
							sell(asset, adjustmentHappeningDate, (float) (triPreviousAverageClose * UHaul.getLowBound2()), positionAssetIterator,
									"HaulingAtHighLow && (intraDayLow <= triPreviousAverageClose * "+UHaul.getLowBound2()+")", batchTimestamp,
									MonteCarloTimestamp);
							System.out.println("asset " + asset.stockCode + " sold on: " + adjustmentHappeningDate
									+ " at " + triPreviousAverageClose * UHaul.getLowBound2()
									+ " due to its intraday gain plummet "+(1-UHaul.getLowBound2())*100+"% of buy price: " + triPreviousAverageClose
									+ " with intraday LOW price " + intraDayLow);
						}
					} else if (!sellingStrategy.equals("MA") && assetUnrealizedChange <= stopLoss || assetUnrealizedChange > elasticRetainGain) {
						System.out.println(
								"reason 0: assetUnrealizedChange <= stopLoss || assetUnrealizedChange > elasticRetainGain");
						reasoned = "SELL Trigger, reason 0: assetUnrealizedChange <= stopLoss || assetUnrealizedChange > elasticRetainGain";
					}  else if (!sellingStrategy.equals("MA") && assetGainSpeed < lowerBoundSpeed) {
						System.out.println("reason 2: assetGainSpeed < lowerBoundSpeed");
						reasoned = "SELL Trigger, reason 2: assetGainSpeed < lowerBoundSpeed";
					} else if (!sellingStrategy.equals("MA") && assetGainSpeed < -averageSpeed
							&& asset.getHoldingDays(adjustmentHappeningDate, simulationDateRange) >= 5) {
						System.out.println("reason 3: assetGainSpeed < averageSpeed && holdingDays  >= 5");
						reasoned = "SELL Trigger, reason 3: assetGainSpeed < averageSpeed && holdingDays  >= 5";
					} 
//					else if (assetUnrealizedChange < 0 
//							//&& assetUnrealizedChange < elasticRetainGain
//							&& asset.getHoldingDays(adjustmentHappeningDate, simulationDateRange) >= 10) {
//						System.out.println(
//								"reason 1: assetUnrealizedChange <= stopLoss || assetUnrealizedChange * 1.5 > elasticRetainGain * 1.5");
//						reasoned = "SELL Trigger, reason 1: assetUnrealizedChange < 0 || HoldingDays >= 10";
//					}
					else if (!sellingStrategy.equals("MA") && assetUnrealizedChange < elasticRetainGain
							&& asset.getHoldingDays(adjustmentHappeningDate, simulationDateRange) >= 20) {
						System.out.println("reason 4: assetUnrealizedChange < elasticRetainGain && holdingDays  >= 20");
						reasoned = "SELL Trigger, reason 4: assetUnrealizedChange < elasticRetainGain && holdingDays  >= 20";
					}
//					else if (assetUnrealizedGL < -faultTradeTolerance) {
//						System.out.println("reason 5: assetUnrealizedGL < "+ -faultTradeTolerance);
//						reasoned = "SELL Trigger, reason 5: assetUnrealizedGL < "+ -faultTradeTolerance;
//					} 
 
					System.out.println("reasoned: " + reasoned);
					if (adjustmentHappeningDate.isAfter(asset.buyDate) && reasoned.startsWith("SELL Trigger")) {
						sell(asset, adjustmentHappeningDate, asset.getSelectDate(), positionAssetIterator, "CLOSE",
								reasoned, batchTimestamp, MonteCarloTimestamp);
						System.out.println("asset " + asset.stockCode + " sold on: " + adjustmentHappeningDate + " at "
								+ getAssetPrice(asset.stockCode, asset.selectDate, adjustmentHappeningDate, "CLOSE")
								+ " due to: " + reasoned);
					}
				}
			}
		}

		int howManyAssetsToBuy = assetsToBuyWithSelectDate.size();
		while (toBuyIterator.hasNext()) {
			Asset assetToBuy = toBuyIterator.next();
			assetCode = assetToBuy.getStockCode();
			selectLocalDate = assetToBuy.getSelectDate();
			System.out.println("selectLocalDate: " + selectLocalDate);
			assetBuyPrice = getAssetPrice(assetCode, selectLocalDate, adjustmentHappeningDate, "OPEN");
			System.out.println("toBuy asset: " + assetCode + ", asset assetBuyPrice: " + assetBuyPrice);
			if (assetBuyPrice <= 0) {
				System.out.println(assetToBuy + " has NO price info on " + adjustmentHappeningDate
						+ ", skipping buying this asset on this day...");
				howManyAssetsToBuy--;
				continue;
			}
			if (howManyAssetsToBuy > 0)
				eachPositionAmount = cashBalance / howManyAssetsToBuy;
			if (eachPositionAmount > positionScale * initialCashInvestment)
				eachPositionAmount = positionScale * initialCashInvestment;
			ComputingQuantity = (float) Math.floor(eachPositionAmount / assetBuyPrice);
			System.out.println("ComputingQuantity of " + assetToBuy + " is :" + ComputingQuantity + " shares.");
			System.out.println("eachPositionAmount of " + assetToBuy + " is :" + eachPositionAmount);
			Asset toBuyAsset = new Asset(assetCode);
			toBuyAsset.setSelectDate(selectLocalDate);
			// risingCount = getAssetHistory(toBuyAsset, adjustmentHappeningDate, 8)[0];
			// fallingCount = getAssetHistory(toBuyAsset, adjustmentHappeningDate, 8)[1];
			System.out.println("asset: " + toBuyAsset.stockCode + " on " + adjustmentHappeningDate
			// + ", Rising: "+ risingCount + ", Falling: " + fallingCount
			);
			assetSelectClose = getAssetPrice(assetCode, selectLocalDate, selectLocalDate, "CLOSE");
			if (strategy.equals("BETTERBUYINGDAY")) {
				if (adjustmentHappeningDate.isAfter(betterBuyingDay) && ComputingQuantity >= 100
				// && risingCount <= 1
				// && fallingCount <= 1
				) {
					if((bullishPredicted==false) || (bullishPredicted==true && YINNChangeDaily>-0.02)) {
					buy(toBuyAsset, adjustmentHappeningDate, toBuyAsset.getSelectDate(), assetSelectClose,
							ComputingQuantity, "OPEN", batchTimestamp, MonteCarloTimestamp, YINNChangeDaily);
					}
				}
			} else if (strategy.equals("LOWPRICEDAY")) {
				if (assetBuyPrice < 0.9 * assetSelectClose && ComputingQuantity >= 100
				// && risingCount <= 1 && fallingCount <= 1)
				) {
					if((bullishPredicted==false) || (bullishPredicted==true && YINNChangeDaily>-0.02)) {
					buy(toBuyAsset, adjustmentHappeningDate, toBuyAsset.getSelectDate(), assetSelectClose,
							ComputingQuantity, "OPEN", batchTimestamp, MonteCarloTimestamp, YINNChangeDaily);
					}
				}
			} else {
				if (adjustmentHappeningDate.isAfter(betterBuyingDay) && ComputingQuantity >= 100) {
					if((bullishPredicted==false) || (bullishPredicted==true && YINNChangeDaily>-0.02)) {
					buy(toBuyAsset, adjustmentHappeningDate, toBuyAsset.getSelectDate(), assetSelectClose,
							ComputingQuantity, "OPEN", batchTimestamp, MonteCarloTimestamp, YINNChangeDaily);
					}
				}
			}
		}
		accountingPortfolioBalances(adjustmentHappeningDate, "CLOSE");
		hibernatePositions(assetList, adjustmentHappeningDate, sellOff, batchTimestamp, MonteCarloTimestamp, conn,
				stmt);
	}

	void runALL(LocalDate start, LocalDate end, int sublistSize, double positionScale, double sellOffThreshold,
			double stopLoss, double retainGain, double lowerBoundSpeed, double averageSpeed, int vintage,
			String strategy, String intraDayStrategy, String sellingStrategy, int volatilityAcceptanceNumber, 
			double faultTradeTolerance, boolean bullishPredicted,
			Timestamp batchTimestamp, Timestamp MonteCarloTimestamp, Connection conn, Statement stmt) {
		LocalDateTime simulationStartTime = LocalDateTime.now();
		// LocalDate lastPickDateLocal;
		List<LocalDate> simulationDateRange = setSimulationDateRange(start, end);
		List<String> dwellerList, subList, pool;
		LocalDate betterBuyingDay = null;
		AssetBuyingPlan abp;
		// String buffer;
		Asset bufferAsset;
		Vector<Map<String, java.util.Date>> assetStack;
		Vector<Map<String, java.util.Date>> assetStackCopy;
		Map<String, java.util.Date> assetInStack;
		String assetInStackCode = null;
		LocalDate selectDate;
		java.util.Date selectDateInMap;
		List<Asset> subListAssets;
		try {
			System.out.println("initialize access: ");
			abp = accessABP(start, start, "FJQ_PICK", vintage, false, conn, stmt);
			// System.out.println("abp: " + abp);
			assetStack = accessAssetStack(start, start, "FJQ_PICK", conn, stmt);
			assetStackCopy = accessAssetStack(start, start, "FJQ_PICK", conn, stmt);
			System.out.println("assetStack has: " + assetStack.size() + " asset(s).");
			System.out.println("assetStack: " + assetStack);

			for (LocalDate adjustmentHappeningDay : simulationDateRange) {
				assetStack = accessAssetStack(adjustmentHappeningDay, start, "FJQ_PICK", conn, stmt);
				assetStackCopy = accessAssetStack(adjustmentHappeningDay, start, "FJQ_PICK", conn, stmt);
				subList = new LinkedList<String>();
				pool = new LinkedList<String>();
				dwellerList = new LinkedList<String>();
				for (Asset a : assetList) {
					dwellerList.add(a.getStockCode());
				}

				System.out.println("assetStack has: " + assetStack.size() + " asset(s).");
				System.out.println("assetStack: " + assetStack);

				/*
				 * 
				 * System.out.println("simulation day: " + s); samplingList = new
				 * LinkedList<String>(); joinList = new LinkedList<String>()
				 * 
				 * // Stack<Map<String, Map<Date, PriceNDesc[]>>> for (Map<String,
				 * java.util.Date> a : abp.pickDate) { stockCodeKeyinMap = (String)
				 * a.keySet().toArray()[0]; joinList.add(stockCodeKeyinMap); }
				 * joinList.retainAll(dwellerList); for (Map<String, java.util.Date> a :
				 * abp.pickDate) { if (!joinList.contains((String) a.keySet().toArray()[0])) {
				 * stockCodeKeyinMap = (String) a.keySet().toArray()[0];
				 * samplingList.add(stockCodeKeyinMap); } } sampleSize = samplingList.size(); if
				 * (sampleSize > 0) { for (int i = 0; i < sublistSize; i++) { int r = (int)
				 * Math.floor(Math.random() * sampleSize); while
				 * (subList.contains(samplingList.get(r))) { r = (int) Math.floor(Math.random()
				 * sampleSize); } buffer = samplingList.get(r); bufferAsset = new Asset(buffer);
				 * bufferAsset.setSelectDate(abp.pickDate); if (getAssetHistory(bufferAsset,
				 * abp.pickDate, 8)[0] <= 0 && getAssetHistory(bufferAsset, abp.pickDate, 8)[1]
				 * <= 0) { subList.add(samplingList.get(r)); } if (!(subList.size() <
				 * sampleSize)) break; } } if (subList.size() >= sublistSize) { break; }
				 * 
				 */

				while (!assetStack.isEmpty()) {
					assetInStack = ((Stack<Map<String, java.util.Date>>) assetStack).pop();

					System.out.println("asset In Stack: " + assetInStack);

					if (assetInStack == null) {
						break;
					}

					for (String aiq : assetInStack.keySet()) {
						assetInStackCode = aiq;
					}
					if (!dwellerList.contains(assetInStackCode) && !pool.contains(assetInStackCode)) {
						bufferAsset = new Asset(assetInStackCode);
						System.out.println(assetInStack);
						selectDate = LocalDate.parse(assetInStack.get(assetInStackCode).toString());
						bufferAsset.setSelectDate(selectDate);
						System.out.println(assetInStackCode + " on " + adjustmentHappeningDay
								+ "， getAssetHistory(bufferAsset, adjustmentHappeningDay, 8)[0]: "
								+ getAssetHistory(bufferAsset, bufferAsset.getSelectDate(), 8)[0]);
						System.out.println(assetInStackCode + " on " + adjustmentHappeningDay
								+ "， getAssetHistory(bufferAsset, adjustmentHappeningDay, 8)[1]: "
								+ getAssetHistory(bufferAsset, adjustmentHappeningDay, 8)[1]);
						if (getAssetHistory(bufferAsset, bufferAsset.getSelectDate(),
								8)[0] <= volatilityAcceptanceNumber
								&& getAssetHistory(bufferAsset, adjustmentHappeningDay,
										8)[1] <= volatilityAcceptanceNumber) {
							pool.add(assetInStackCode);
						}
					}
				}

				while (!pool.isEmpty()) {
					int r = (int) Math.floor(Math.random() * pool.size());
					if (subList.size() >= pool.size()) {
						break;
					}
					if (subList.size() >= sublistSize) {
						break;
					}
					while (subList.contains((pool.get(r)))) {
						r = (int) Math.floor(Math.random() * pool.size());
					}
					subList.add(pool.get(r));
				}

				System.out.println("subList Assets: " + subList.toString());
				// betterBuyingDay = abp.vintageDate;
				// subListAssets = new LinkedList<Map<String, java.util.Date>>();
				subListAssets = new LinkedList<Asset>();
				Map<String, Map<String, java.util.Date>> mapWithSelectDate = new HashMap<String, Map<String, java.util.Date>>();

				for (Map<String, java.util.Date> m : assetStackCopy) {
					for (String item : subList) {
						if (m.containsKey(item)) {
							selectDateInMap = m.get(item);
							if (mapWithSelectDate.containsKey(item)
									&& selectDateInMap.after(mapWithSelectDate.get(item).get(item))) {
								mapWithSelectDate.put(item, m);
							} else if (!mapWithSelectDate.containsKey(item)) {
								mapWithSelectDate.put(item, m);
							}
						}
					}
				}

				for (Entry<String, Map<String, java.util.Date>> e : mapWithSelectDate.entrySet()) {
					// Map<String, java.util.Date> tinyMap = new HashMap<String, java.util.Date>();
					// tinyMap.put(e.getKey(), e.getValue().get(e.getKey()));
					Asset assetInSubList = new Asset(e.getKey());
					assetInSubList.setSelectDate(LocalDate.parse(e.getValue().get(e.getKey()).toString()));
					subListAssets.add(assetInSubList);
				}

				System.out.println("on simulation day: " + adjustmentHappeningDay + ", subListAssets Assets: "
						+ subListAssets.toString());

				betterBuyingDay = abp.vintageDate;
				adjustment(subListAssets, adjustmentHappeningDay, betterBuyingDay, false, positionScale,
						sellOffThreshold, stopLoss, retainGain, lowerBoundSpeed, averageSpeed, strategy,
						intraDayStrategy, sellingStrategy, volatilityAcceptanceNumber, faultTradeTolerance, 
						bullishPredicted, batchTimestamp, MonteCarloTimestamp, conn, stmt);
				System.out.println("at the end of day...: " + adjustmentHappeningDay);
				System.out.println("re-entering access: " + adjustmentHappeningDay);
				LocalDate nextPickDate;
				if (abp.nextPickDate != null) {
					System.out.println(abp.nextPickDate.toString());
					nextPickDate = LocalDate.parse(abp.nextPickDate.toString());
				} else {
					nextPickDate = LocalDate.parse(abp.pickDate.toString());
				}
				if (!adjustmentHappeningDay.isBefore(nextPickDate)) {
					abp = accessABP(adjustmentHappeningDay, start, "FJQ_PICK", vintage, true, conn, stmt);
					System.out.println("assetStack: " + assetStack);
				}
			}
		} catch (Exception e) {
			System.out.println("Exception in simulationRun(): ");
			e.printStackTrace();
		} finally {
			LocalDateTime accessEndTime = LocalDateTime.now();
			System.out.println("End local time: " + LocalDateTime.now());
			System.out.println("FJQ simulation duration: "
					+ simulationStartTime.until(accessEndTime, ChronoUnit.SECONDS) + " Seconds.");
		}
		return;
	}

	void runLASTONLY2(LocalDate start, LocalDate end, int sublistSize, double positionScale, double sellOffThreshold,
			double stopLoss, double retainGain, double lowerBoundSpeed, double averageSpeed, int vintage,
			String strategy, String intraDayStrategy, String sellingStrategy, int volatilityAcceptanceNumber, 
			double faultTradeTolerance, boolean bullishPredicted,
			Timestamp batchTimestamp, Timestamp MonteCarloTimestamp, Connection conn, Statement stmt) {
		// LocalDate lastPickDateLocal;
		List<LocalDate> simulationDateRange = setSimulationDateRange(start, end);
		List<String> dwellerList, joinList, samplingList, subList;
		LocalDate betterBuyingDay = null;
		AssetBuyingPlan abp;
		// String buffer;
		Asset bufferAsset;
		String stockCodeKeyinMap = null, buffer;
		List<Asset> subListAssets;
		try {
			System.out.println("Iterating trading days: ");
			for (LocalDate adjustmentHappeningDay : simulationDateRange) {
				System.out.println("on simulation day: " + adjustmentHappeningDay);
				for (Asset a : assetList) {
					System.out.println(a.getStockCode());
				}
				abp = accessABP(adjustmentHappeningDay, start, "FJQ_PICK", vintage, false, conn, stmt);				
				
				subList = new LinkedList<String>();
				dwellerList = new LinkedList<String>();
				for (Asset a : assetList) {
					dwellerList.add(a.getStockCode());
				}
				samplingList = new LinkedList<String>();
				joinList = new LinkedList<String>();
				for (Map<String, Float> a : abp.listOfAssets) {
					stockCodeKeyinMap = (String) a.keySet().toArray()[0];
					joinList.add(stockCodeKeyinMap);
				}
				joinList.retainAll(dwellerList);
				for (Map<String, Float> a : abp.listOfAssets) {
					if (!joinList.contains((String) a.keySet().toArray()[0])) {
						stockCodeKeyinMap = (String) a.keySet().toArray()[0];
						samplingList.add(stockCodeKeyinMap);
					}
				}
				int sampleSize = samplingList.size();
				if (sampleSize > 0) {
					for (int i = 0; i < sublistSize; i++) {
						int r = (int) Math.floor(Math.random() * sampleSize);
						while (subList.contains(samplingList.get(r))) {
							r = (int) Math.floor(Math.random() * sampleSize);
						}
						buffer = samplingList.get(r);
						bufferAsset = new Asset(buffer);
						bufferAsset.setSelectDate(abp.pickDate);
						System.out.println(buffer + " on " + adjustmentHappeningDay
								+ "， getAssetHistory(bufferAsset, adjustmentHappeningDay, 8)[0]: "
								+ getAssetHistory(bufferAsset, bufferAsset.getSelectDate(), 8)[0]);
						System.out.println(buffer + " on " + adjustmentHappeningDay
								+ "， getAssetHistory(bufferAsset, adjustmentHappeningDay, 8)[1]: "
								+ getAssetHistory(bufferAsset, adjustmentHappeningDay, 8)[1]);
						if (getAssetHistory(bufferAsset, abp.pickDate, 8)[0] <= volatilityAcceptanceNumber
								&& getAssetHistory(bufferAsset, abp.pickDate, 8)[1] <= volatilityAcceptanceNumber) {
							subList.add(samplingList.get(r));
						}
						if ((subList.size() >= sampleSize || subList.size() >= sublistSize)) {
							break;
						}
					}
				}

				subListAssets = new LinkedList<Asset>();
				for (String e : subList) {
					Asset assetInSubList = new Asset(e);
					assetInSubList.setSelectDate(abp.pickDate);
					// assetInSubList.setVinatgeDate(betterBuyingDay);
					subListAssets.add(assetInSubList);
				}
				betterBuyingDay = abp.vintageDate;
				for (Asset s : subListAssets) {
					System.out.println("on simulation day: " + adjustmentHappeningDay + ", subList Assets: "
							+ s.getStockCode().toString() + " sublisted on " + s.getSelectDate() + " with selectDate "
							+ s.getSelectDate() + ", and abp vintage date: " + betterBuyingDay);
				}

				adjustment(subListAssets, adjustmentHappeningDay, betterBuyingDay, false, positionScale,
						sellOffThreshold, stopLoss, retainGain, lowerBoundSpeed, averageSpeed, strategy,
						intraDayStrategy, sellingStrategy, volatilityAcceptanceNumber, faultTradeTolerance, 
						bullishPredicted, batchTimestamp, MonteCarloTimestamp, conn, stmt);
				System.out.println("at the end of day...: " + adjustmentHappeningDay);
				System.out.println("re-entering access: " + adjustmentHappeningDay);
				LocalDate nextPickDate;
				if (abp.nextPickDate != null) {
					System.out.println(abp.nextPickDate.toString());
					nextPickDate = LocalDate.parse(abp.nextPickDate.toString());
				} else {
					nextPickDate = LocalDate.parse(abp.pickDate.toString());
				}
//				if (!adjustmentHappeningDay.isBefore(nextPickDate)) {
//					abp = accessABP(adjustmentHappeningDay, start, "FJQ_PICK", vintage, conn, stmt);
//				}
			}
		} catch (Exception e) {
			System.out.println("Exception in runLASTONLY(): ");
			e.printStackTrace();
		} finally {
		}
		return;
	}

	void runLASTONLY(LocalDate start, LocalDate end, int sublistSize, double positionScale, double sellOffThreshold,
			double stopLoss, double retainGain, double lowerBoundSpeed, double averageSpeed, int vintage,
			String strategy, String intraDayStrategy, String sellingStrategy, int volatilityAcceptanceNumber, 
			double faultTradeTolerance, boolean bullishPredicted,
			Timestamp batchTimestamp, Timestamp MonteCarloTimestamp, Connection conn, Statement stmt) {
		// LocalDate lastPickDateLocal;
		List<LocalDate> simulationDateRange = setSimulationDateRange(start, end);
		List<String> dwellerList, joinList, samplingList, subList;
		LocalDate betterBuyingDay = null;
		AssetBuyingPlan abp, vintagedABP;
		// String buffer;
		Asset bufferAsset;
		String stockCodeKeyinMap = null, buffer;
		List<Asset> subListAssets;
		try {
			System.out.println("Iterating trading days: ");
			for (LocalDate adjustmentHappeningDay : simulationDateRange) {
				System.out.println("on simulation day: " + adjustmentHappeningDay);
				for (Asset a : assetList) {
					System.out.println(a.getStockCode());
				}
				abp = accessABP(adjustmentHappeningDay, start, "FJQ_PICK", vintage, false, conn, stmt);
				Set<LocalDate> vintageDates=vintageSchedule.keySet();
				List<LocalDate> vintageDatesList= new LinkedList<LocalDate>();
				for (LocalDate d: vintageDates) {
					vintageDatesList.add(d);
				}
				Collections.sort(vintageDatesList);
				LocalDate lastVintageDate = LocalDate.now();
					for (LocalDate d: vintageDatesList) {
						if (d.isBefore(adjustmentHappeningDay)) {
							lastVintageDate=d;}
						else {
						}
				}
				
				vintagedABP = vintageSchedule.get(lastVintageDate);

				//vintagedABP = vintageSchedule.get(adjustmentHappeningDay);

				if (vintagedABP != null && !vintagedABP.listOfAssets.isEmpty()) {
					subList = new LinkedList<String>();
					dwellerList = new LinkedList<String>();
					for (Asset a : assetList) {
						dwellerList.add(a.getStockCode());
					}
					samplingList = new LinkedList<String>();
					joinList = new LinkedList<String>();
					for (Map<String, Float> a : vintagedABP.listOfAssets) {
						if(!a.keySet().isEmpty()) {
						stockCodeKeyinMap = (String) a.keySet().toArray()[0];
						joinList.add(stockCodeKeyinMap);
						}
					}
					joinList.retainAll(dwellerList);
					for (Map<String, Float> a : vintagedABP.listOfAssets) {
						if(!a.keySet().isEmpty()) {
						if (!joinList.contains((String) a.keySet().toArray()[0])) {
							stockCodeKeyinMap = (String) a.keySet().toArray()[0];
							samplingList.add(stockCodeKeyinMap);
						}
						}
					}
					int sampleSize = samplingList.size();
					if (sampleSize > 0) {
						for (int i = 0; i < sublistSize; i++) {
							int r = (int) Math.floor(Math.random() * sampleSize);
							while (subList.contains(samplingList.get(r))) {
								r = (int) Math.floor(Math.random() * sampleSize);
							}
							buffer = samplingList.get(r);
							bufferAsset = new Asset(buffer);
							bufferAsset.setSelectDate(vintagedABP.pickDate);
							System.out.println(buffer + " on " + adjustmentHappeningDay + "， rising count: "
									+ getAssetHistory(bufferAsset, bufferAsset.getSelectDate(), 8)[0]);
							System.out.println(buffer + " on " + adjustmentHappeningDay + "， falling count: "
									+ getAssetHistory(bufferAsset, adjustmentHappeningDay, 8)[1]);
							if (getAssetHistory(bufferAsset, bufferAsset.getSelectDate(),
									8)[0] <= volatilityAcceptanceNumber
									&& getAssetHistory(bufferAsset, bufferAsset.getSelectDate(),
											8)[1] <= volatilityAcceptanceNumber) {
								subList.add(samplingList.get(r));
							}
							if ((subList.size() >= sampleSize || subList.size() >= sublistSize)) {
								break;
							}
						}
					}

					subListAssets = new LinkedList<Asset>();
					for (String e : subList) {
						Asset assetInSubList = new Asset(e);
						assetInSubList.setSelectDate(vintagedABP.pickDate);
						// assetInSubList.setVinatgeDate(betterBuyingDay);
						subListAssets.add(assetInSubList);
					}
					betterBuyingDay = vintagedABP.vintageDate;
					for (Asset s : subListAssets) {
						System.out.println("on simulation day: " + adjustmentHappeningDay + ", subList Assets: "
								+ s.getStockCode().toString() + " sublisted on " + s.getSelectDate()
								+ " with selectDate " + s.getSelectDate() + ", and abp vintage date: "
								+ betterBuyingDay);
					}
				} else {
					subListAssets = new LinkedList<Asset>();
				}
				adjustment(subListAssets, adjustmentHappeningDay, betterBuyingDay, false, positionScale,
						sellOffThreshold, stopLoss, retainGain, lowerBoundSpeed, averageSpeed, strategy,
						intraDayStrategy, sellingStrategy, volatilityAcceptanceNumber, faultTradeTolerance, 
						bullishPredicted, batchTimestamp, MonteCarloTimestamp, conn, stmt);
				System.out.println("at the end of day...: " + adjustmentHappeningDay);
				System.out.println("re-entering access: " + adjustmentHappeningDay);
				LocalDate nextPickDate;
				if (abp.nextPickDate != null) {
					System.out.println(abp.nextPickDate.toString());
					nextPickDate = LocalDate.parse(abp.nextPickDate.toString());
				} else {
					nextPickDate = LocalDate.parse(abp.pickDate.toString());
				}
			}
		} catch (Exception e) {
			System.out.println("Exception in runLASTONLY2(): ");
			e.printStackTrace();
		} finally {
		}
		return;
	}

	Map<java.util.Date, List<TradingPoint>> firstRisingGainLossCal(java.util.Date risingDay, String EntryStrategy,
			String exitStrategy, Connection conn, Statement stmt, Map<java.util.Date, List<TradingPoint>> gainMap) {
		String get_next2_trading_dates = "select distinct 交易日期 from daily_snapshot where 交易日期 > '" + risingDay
				+ "' order by 交易日期  limit 2;";
		java.util.Date d1;
		ResultSet rs2;
		List<java.util.Date> tradingPeriod = new LinkedList<java.util.Date>();
		List<TradingPoint> buyingBatch = new LinkedList<TradingPoint>();
		try {
			rs2 = stmt.executeQuery(get_next2_trading_dates);
			while (rs2.next()) {
				d1 = rs2.getDate("交易日期");
				tradingPeriod.add(d1);
			}
			Date buyingDay = (Date) tradingPeriod.get(0);
			Date sellingDay = (Date) tradingPeriod.get(1);
			String buying_price = "select * from first_rising where first_rising_date='" + risingDay
					+ "' and next_day_1030_open > close_price "
					+ " and next_day_1030_close > close_price and next_day_1030_open!=next_day_1030_close and next_day_1030_open<close_price*1.08";

			ResultSet rs = stmt.executeQuery(buying_price);
			while (rs.next()) {
				TradingPoint bp = new TradingPoint();
				bp.stockCode = rs.getString("stock_code");
				bp.stockName = rs.getString("stock_name");
				// bp.buyDate=rs.getDate("交易日期");
				bp.buyingPrice = (rs.getDouble("next_day_1030_open") + rs.getDouble("next_day_1030_close")) / 2;
				buyingBatch.add(bp);
			}
			List<TradingPoint> forADay = new LinkedList<TradingPoint>();

			for (TradingPoint sp : buyingBatch) {
				String sell_price = "select * from daily_snapshot where 交易日期='" + sellingDay + "' and 股票代码='"
						+ sp.stockCode + "'";
				ResultSet rs1 = stmt.executeQuery(sell_price);
				while (rs1.next()) {
					sp.sellingPriceClose = rs1.getDouble("收盘价");
					sp.sellingPriceOpen = rs1.getDouble("开盘价");
					sp.sellingPriceHigh = rs1.getDouble("最高价");
					sp.sellingPriceHighLowAverage = (rs1.getDouble("最高价") + rs1.getDouble("最低价")) / 2;
					sp.sellDate = sellingDay;
					sp.gainRateOpen = (sp.sellingPriceOpen - sp.buyingPrice) / sp.buyingPrice;
					sp.gainRateClose = (sp.sellingPriceClose - sp.buyingPrice) / sp.buyingPrice;
					sp.gainRateHigh = (sp.sellingPriceHigh - sp.buyingPrice) / sp.buyingPrice;
					sp.gainRateHighLowAverage = (sp.sellingPriceHighLowAverage - sp.buyingPrice) / sp.buyingPrice;
					System.out.println(
							"DEBUG 1... 股票:" + sp.stockName + ", sellingDay: " + sellingDay + ", sellingPriceClose: "
									+ sp.sellingPriceClose + ", buyingPrice: " + sp.buyingPrice + ", Rate(Close): "
									+ sp.gainRateClose + ", Rate(Open): " + sp.gainRateOpen + ", Rate(High): "
									+ sp.gainRateHigh + ", Rate(HighLowAverage): " + sp.gainRateHighLowAverage);
					forADay.add(sp);
				}
			}
			gainMap.putIfAbsent(buyingDay, forADay);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return gainMap;
	}
}

/* index id below, perform SQL clause below to update daily_snapshot table, is_index field if necessary. 
 * update daily_snapshot set is_index='Y' where 股票代码 in ('sh000001', 'sh000002',
 * 'sh000003', 'sh000004', 'sh000005', 'sh000006', 'sh000007', 'sh000008',
 * 'sh000009', 'sh000010', 'sh000016', 'sh000017', 'sh000020', 'sh000032',
 * 'sh000033', 'sh000034', 'sh000035', 'sh000036', 'sh000037', 'sh000038',
 * 'sh000039', 'sh000040', 'sh000041', 'sh000043', 'sh000044', 'sh000045',
 * 'sh000046', 'sh000047', 'sh000090', 'sh000104', 'sh000105', 'sh000106',
 * 'sh000107', 'sh000108', 'sh000109', 'sh000110', 'sh000111', 'sh000112',
 * 'sh000113', 'sh000132', 'sh000133', 'sh000155', 'sh000300', 'sh000852',
 * 'sh000903', 'sh000904', 'sh000905', 'sh000906', 'sz399001', 'sz399002',
 * 'sz399003', 'sz399004', 'sz399005', 'sz399006', 'sz399007', 'sz399008',
 * 'sz399009', 'sz399010', 'sz399011', 'sz399012', 'sz399015', 'sz399300',
 * 'sz399903', 'sz399905')
 */
