package StockMarket;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

public class Ramble {
	public static void main(String[] args) {
		try {
			new ThruBreaker();
			java.sql.Date TStartDate = java.sql.Date.valueOf("2025-03-19");
			java.sql.Date TEndDate = java.sql.Date.valueOf("2025-03-28");
			String stocksSql = "select * from volatility where volatility_5_standardized > 0.04 and trade_date ='2025-03-18'";
			Statement stmt;
			stmt = ThruBreaker.conn.createStatement();
			ResultSet rs = stmt.executeQuery(stocksSql);
			String stock;
			List<String> stockCodes = new ArrayList<String>();
			while (rs.next()) {
				stock = rs.getString("stock_code");
				stockCodes.add(stock);
			}
			Asset[] trendingStocks = new Asset[stockCodes.size()];
			for (int i = 0; i < trendingStocks.length; i++) {
				trendingStocks[i] = new Asset(stockCodes.get(i));
			}
			LocalDate priceStart = LocalDate.of(2024, 12, 1);
			LocalDate priceTill = LocalDate.now();
			ThruBreaker.setPriceDateRange(priceStart, priceTill);
			new ThruBreaker();
			float high, low, open, close, tBuyPrice, tSellPrice, exitPrice , entrancePrice = 0;
			double signleDayTGain, buyPoint = 0.1, sellPoint = 0.9, singleCost = 0, cost, GL;
			float gain;
			int NoOfShares = 2000;
			ThruBreaker.setAllAssetPricesMappingBasic(priceStart, priceTill, ThruBreaker.conn,
					ThruBreaker.conn.createStatement());

			for (int j = 0; j < (trendingStocks.length); j++) {
				cost = 0;
				GL = 0;
				for (LocalDate TtradeDay : ThruBreaker.priceDateRange) {
					if (!TtradeDay.isBefore(TStartDate.toLocalDate()) && !TtradeDay.isAfter(TEndDate.toLocalDate())) {
						if (TtradeDay.equals(TStartDate.toLocalDate())) {
							singleCost = ThruBreaker.allAssetPricesMapping.get(trendingStocks[j].stockCode)
									.get(ThruBreaker.accessPriceDateRange(TtradeDay, 0)).closePrice * NoOfShares;
							entrancePrice = ThruBreaker.allAssetPricesMapping.get(trendingStocks[j].stockCode)
									.get(ThruBreaker.accessPriceDateRange(TtradeDay, 0)).closePrice;
							System.out.println(trendingStocks[j].stockCode + " on " + TtradeDay + "'s entrance Price is: " + entrancePrice);
						}
						high = ThruBreaker.allAssetPricesMapping.get(trendingStocks[j].stockCode)
								.get(ThruBreaker.accessPriceDateRange(TtradeDay, 0)).highPrice;
						low = ThruBreaker.allAssetPricesMapping.get(trendingStocks[j].stockCode)
								.get(ThruBreaker.accessPriceDateRange(TtradeDay, 0)).lowPrice;
						close = ThruBreaker.allAssetPricesMapping.get(trendingStocks[j].stockCode)
								.get(ThruBreaker.accessPriceDateRange(TtradeDay, 0)).closePrice;
						open = ThruBreaker.allAssetPricesMapping.get(trendingStocks[j].stockCode)
								.get(ThruBreaker.accessPriceDateRange(TtradeDay, 0)).openPrice;
						tBuyPrice = (float) (low + (high - low) * buyPoint);
						tSellPrice = (float) (low + (high - low) * sellPoint);
						signleDayTGain = NoOfShares * (tSellPrice - tBuyPrice);
						gain =(float) + signleDayTGain;
						System.out.println(trendingStocks[j].stockCode + " on " + TtradeDay + "'s gain is: " + (float)signleDayTGain);
						
						if (ThruBreaker.accessPriceDateRange(TEndDate.toLocalDate(), 0).equals(ThruBreaker.accessPriceDateRange(TtradeDay, 0))){
							exitPrice = ThruBreaker.allAssetPricesMapping.get(trendingStocks[j].stockCode)
									.get(ThruBreaker.accessPriceDateRange(TtradeDay, 0)).closePrice;
							GL = (float) - singleCost + gain + NoOfShares * exitPrice;
							System.out.println(trendingStocks[j].stockCode + " on " + TtradeDay + "'s entrance Price is: " + entrancePrice);
							System.out.println(trendingStocks[j].stockCode + " on " + TtradeDay + "'s exit Price is: " + exitPrice);
							System.out.println(trendingStocks[j].stockCode + "'s cost is: " + singleCost);
							System.out.println(trendingStocks[j].stockCode + "'s total gain is: " + GL);
							break;
						}
					}
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
