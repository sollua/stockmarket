package StockMarket;

import java.lang.reflect.Type;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class UnitTest {
	public static double round(double value, int places) {
		if (places < 0)
			throw new IllegalArgumentException();

		long factor = (long) Math.pow(10, places);
		value = value * factor;
		long tmp = Math.round(value);
		return (double) tmp / factor;
	}

	public static void main(String[] args) {
		new ThruBreaker();
		LocalDate prior, posterior, computingDate;
		Double deviation;
		List<LocalDate> priceDateRange = ThruBreaker.setPriceDateRange(LocalDate.of(2024, 01, 01), LocalDate.now());
		String deviationBasedTrendAssetAnalysisSQL;
		ResultSet rs;
		Statement stmt;
		prior = LocalDate.of(2024, 10, 25);
		
		deviation = 0.11;
		String insertSql = "insert into curve_plane (deviation, prior_date, posterior_date, computing_date, average_return) values(?,?,?,?,?)";
		PreparedStatement ps;
		try {
			ps = ThruBreaker.conn.prepareStatement(insertSql);
			stmt = ThruBreaker.conn.createStatement();
			for (int i = 1; i < 21; i++) {
				for (LocalDate p : priceDateRange) {
					if (!p.isBefore(prior)) {
						for (LocalDate q : priceDateRange) {
							if (q.isAfter(p)) {
								deviationBasedTrendAssetAnalysisSQL = "select avg((posteriorClose-priorClose)/priorClose) average_return from (                                                                                 \n"
										+ " select *, (posteriorClose-priorClose)/priorClose from  (                                                                                \n"
										+ "  ( select 股票代码, 收盘价 posteriorClose from daily_snapshot where 交易日期='"
										+ q.toString()
										+ "' and 股票代码 in (                                                                             \n"
										+ "  select distinct  asset from ( \n" + "   select * from ( \n"
										+ "     select * from (      \n" + "select asset,\n"
										+ "estimate_upToComputeDate,	\n" + "residual_upToComputeDate,	\n"
										+ "price_upToComputeDate,\n"
										+ "computing_date,price_estimate_deviation, round(cast(REPLACE(price_estimate_deviation, '%', '') as double)/100,2) deviation\n"
										+ "from matrix_correlation where computing_date='" + p.toString()
										+ "' ) p where deviation =" + deviation + ") t \n"
										+ "join (select distinct 股票代码, 股票名称 from daily_snapshot) b\n" + "on \n"
										+ "t.asset=b.股票代码) c ) \n" + " )  posterior\n" + " \n" + "join\n"
										+ "   ( select 股票代码 stock_code, 收盘价 priorClose from daily_snapshot where 交易日期='"
										+ p.toString()
										+ "' and 股票代码 in (                                                                             \n"
										+ "  select distinct  asset from ( \n" + "   select * from ( \n"
										+ "     select * from (      \n" + "select asset,\n"
										+ "estimate_upToComputeDate,	\n" + "residual_upToComputeDate,	\n"
										+ "price_upToComputeDate,\n"
										+ "computing_date,price_estimate_deviation, round(cast(REPLACE(price_estimate_deviation, '%', '') as double)/100,2) deviation\n"
										+ "from matrix_correlation where computing_date='" + p.toString()
										+ "' ) p where  deviation =" + deviation + ") t \n"
										+ "join (select distinct 股票代码, 股票名称 from daily_snapshot) b\n" + "on \n"
										+ "t.asset=b.股票代码) c ) \n" + " ) prior  \n"
										+ "on prior.stock_code =posterior.股票代码)) ll;";
								// System.out.println(deviationBasedTrendAssetAnalysisSQL);
								rs = stmt.executeQuery(deviationBasedTrendAssetAnalysisSQL);
								while (rs.next()) {
									System.out.println("deviation - " + deviation + ", prior - " + p + ", posterior - "
											+ q + ": " + rs.getDouble("average_return"));
									ps.setDouble(1, deviation);
									ps.setDate(2, Date.valueOf(p));
									ps.setDate(3, Date.valueOf(q));
									ps.setDate(4, Date.valueOf(p));
									ps.setDouble(5, round(rs.getDouble("average_return")*100, 2));
									ps.execute();
								}
							}
						}
					}
				}
				deviation = round(deviation + 0.01, 2);
			}
			System.out.println("finished");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
