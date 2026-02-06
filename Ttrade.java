package StockMarket;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;

public class Ttrade {

	public static void main(String[] args) {
		try {
			java.sql.Date TstartDate = java.sql.Date.valueOf("2025-03-19");
			String[] stockCodes = { "sz300819", "sz002553", "sz301196" };
			Asset[] trendingStocks = new Asset[stockCodes.length];
			for (int i = 0; i < trendingStocks.length; i++) {
				trendingStocks[i] = new Asset(stockCodes[i]);
			}

			LocalDate priceStart = LocalDate.of(2024, 12, 1);
			LocalDate priceTill = LocalDate.now();
			ThruBreaker.setPriceDateRange(priceStart, priceTill);
			new ThruBreaker();
			float high, low, open, close, tBuyPrice, tSellPrice;
			double signleDayTGain, gain, buyPoint = 0.1, sellPoint = 0.9, singleCost = 0, cost, GL;
			int NoOfShares = 500;
			ThruBreaker.setAllAssetPricesMappingBasic(priceStart, priceTill, ThruBreaker.conn,
					ThruBreaker.conn.createStatement());

			for (int j = 0; j < (trendingStocks.length); j++) {
				cost = 0;
				GL = 0;
				for (LocalDate TtradeDay : ThruBreaker.priceDateRange) {
					if (!TtradeDay.isBefore(TstartDate.toLocalDate())) {
						if (TtradeDay.equals(TstartDate.toLocalDate())) {
							singleCost = ThruBreaker.allAssetPricesMapping.get(trendingStocks[j].stockCode)
									.get(ThruBreaker.accessPriceDateRange(TtradeDay, 0)).closePrice * NoOfShares;
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
						gain = +signleDayTGain;
						System.out.println(trendingStocks[j] + "on " + TtradeDay + "'s gain is: " + signleDayTGain);
						if (ThruBreaker.priceDateRange.lastIndexOf(TtradeDay) == (ThruBreaker.priceDateRange.size())) {
							GL = -singleCost + gain;
							System.out.println(trendingStocks[j] + "'s total gain is: " + GL);
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
