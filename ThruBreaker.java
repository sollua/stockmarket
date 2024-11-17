package StockMarket;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.apache.commons.io.IOUtils;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.commons.math3.stat.regression.SimpleRegression;

class AssetStatus {
	public Date tradeDate;
	public String stockCode;
	public String stockName;
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
	public float MA5Trend;
	public float MA10Trend;
	public float MA20Trend;
	public float MA30Trend;
	public double totalMarketValue;
	public double denseness;
	public double densenessTrend;
	public double densenessWidth;
	public double sparsnessMid;
	public double sparsnessLong;
	public double sparsnessShort;
	public double sparsnessSuperShort;
	public double fiveMinRise;
	public float speed;
	public String exchange;
}

public class ThruBreaker {
	static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://localhost:20/mysql";
	static final String USER = "root";
	static final String PASS = "";

	static List<LocalDate> simulationDateRange, priceDateRange;
	static int todayIndex;
	static int[] MA5Index;
	static int[] MA10Index;
	static int[] MA30Index;
	static Map<String, String> longForms = null;
	static Map<String, String> MAData;
	static Asset UtilityAsset = new Asset();

	static ZoneId defaultZoneId = ZoneId.systemDefault();
	static Map<String, float[]> entireMarketPrevCloseMapMA5, entireMarketPrevCloseMapMA10, entireMarketPrevCloseMapMA20,
			entireMarketPrevCloseMapMA30;
	static Connection conn;
	static java.util.Date maxDateInDailySnapshot;
	static Map<String, Map<java.util.Date, AssetStatus>> allAssetPricesMapping;

	ThruBreaker() {
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(
					"jdbc:mariadb://localhost:20/mysql?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai",
					"root", "");
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public static class SliceAssetStatus {
		double vol;
		double dollarAmount;
		String ts;
		double sliceAvgPrice;
	}

	static volatile boolean scheduledRunAuthorization = false;

	static boolean oneTimeRun = false;

	static Timer timer = null;

	static class TimerHelper extends TimerTask {
		public static int timerRun = 0;

		public void run() {
			LocalTime now = LocalTime.now();
			if ((now.isAfter(LocalTime.of(9, 20)) && now.isBefore(LocalTime.of(11, 35)))
					|| (now.isAfter(LocalTime.of(12, 57)) && now.isBefore(LocalTime.of(15, 10)))) {
				scheduledRunAuthorization = true;
				System.out.println("Timer ran " + ++timerRun + " @" + now);
			}
		}
	}

	static Map<String, String> shortToLong(Connection conn, Statement stmt) {
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

	static Map<String, String> longCodeToName(Connection conn, Statement stmt) {
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


	static List<LocalDate> setSimulationDateRange(LocalDate start, LocalDate end) {
		simulationDateRange = new LinkedList<LocalDate>();
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

	public static void getMAData(int span, Connection conn, Statement stmt) throws IOException {
		String selectLongCodes = "select distinct 股票代码 from daily_snapshot;";
		ResultSet rs;
		MAData = new HashMap<String, String>();
		String longCode;
		int cnt = 0;
		try {
			rs = stmt.executeQuery(selectLongCodes);
			LocalDateTime start = LocalDateTime.now();
			while (rs.next()) {
				longCode = rs.getString("股票代码");
				URL url = new URL(
						"http://api.mairui.club/hszbl/fsjy/" + longCode + "/" + span + "m/cd5268626606b8b4ef");
				String jsonStr = IOUtils.toString(url, "UTF-8");
				String regex = "[\\}]";
				jsonStr = jsonStr.replaceAll("\"", "");
				MAData.put(longCode, jsonStr);
				if (cnt % 100 == 0) {
					System.out.println("getting 5m vol. to synthesize MA5... processing to: " + cnt);
					LocalDateTime current = LocalDateTime.now();
					System.out.println("100 process costs: " + start.until(current, ChronoUnit.SECONDS) + " seconds.");
				}
				cnt++;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static double[] computeSparsness(double[] MA5, double[] MA10, double[] MA20, double[] MA30, String type,
			String span, boolean currentDateIncluded, float MA10Value) {
		int spanOfEntanglement;
		double sparsnessMA5MA10, sparsnessMA10MA20 = 0;
		double[] betweenMA5MA10 = null, betweenMA10MA20 = null;
		double[] betweenMA5MA10Quadratic = null, betweenMA10MA20Quadratic = null;
		double quadraticSumMA5MA10 = 0, quadraticSumMA10MA20 = 0;
		if (span.equals("superShort")) {
			spanOfEntanglement = 3;
		} else if (span.equals("short")) {
			spanOfEntanglement = 5;
		} else if (span.equals("mid")) {
			spanOfEntanglement = 15;
		} else if (span.equals("long")) {
			spanOfEntanglement = 30;
		} else {
			spanOfEntanglement = 5;
		}
		if (currentDateIncluded) {
			for (int i = 0; i < spanOfEntanglement; i++) {
				betweenMA5MA10 = new double[spanOfEntanglement];
				betweenMA10MA20 = new double[spanOfEntanglement];
				betweenMA5MA10[i] = Math.abs(
						MA5[MA5.length - spanOfEntanglement + 1 + i] - MA10[MA10.length - spanOfEntanglement + 1 + i]);
				betweenMA10MA20[i] = Math.abs(MA10[MA10.length - spanOfEntanglement + 1 + i]
						- MA20[MA20.length - spanOfEntanglement + 1 + i]);
			}
		} else {
			betweenMA5MA10 = new double[spanOfEntanglement - 1];
			betweenMA10MA20 = new double[spanOfEntanglement - 1];
			for (int i = 0; i < spanOfEntanglement - 1; i++) {
				betweenMA5MA10[i] = Math.abs(
						MA5[MA5.length - spanOfEntanglement + 1 + i] - MA10[MA10.length - spanOfEntanglement + 1 + i]);
				betweenMA10MA20[i] = Math.abs(MA10[MA10.length - spanOfEntanglement + 1 + i]
						- MA20[MA20.length - spanOfEntanglement + 1 + i]);
			}
		}

		betweenMA5MA10Quadratic = new double[betweenMA5MA10.length];
		betweenMA10MA20Quadratic = new double[betweenMA10MA20.length];

		if (type.equals("linear")) {
			sparsnessMA5MA10 = UtilityAsset.trend(betweenMA5MA10)[0];
			sparsnessMA10MA20 = UtilityAsset.trend(betweenMA10MA20)[0];
		} else if (type.equals("quadratic")) {
			for (int i = 0; i < betweenMA5MA10.length; i++) {
				betweenMA5MA10Quadratic[i] = Math.pow(betweenMA5MA10[i], 2);
				quadraticSumMA5MA10 += betweenMA5MA10Quadratic[i];
			}
			sparsnessMA5MA10 = Math.sqrt(quadraticSumMA5MA10 / betweenMA5MA10.length) / MA10Value;
			for (int i = 0; i < betweenMA10MA20.length; i++) {
				betweenMA10MA20Quadratic[i] = Math.pow(betweenMA10MA20[i], 2);
				quadraticSumMA10MA20 += betweenMA10MA20Quadratic[i];
			}
			sparsnessMA5MA10 = Math.sqrt(quadraticSumMA5MA10 / betweenMA5MA10.length) / MA10Value;
			sparsnessMA10MA20 = Math.sqrt(quadraticSumMA10MA20 / betweenMA10MA20.length) / MA10Value;
		} else {
			sparsnessMA5MA10 = 1;
			sparsnessMA10MA20 = 1;
		}
		double[] sparnessArray = { sparsnessMA5MA10, sparsnessMA10MA20 };
		return sparnessArray;
	}

	public static double computeChipDenseness(double[] MA5s, double[] MA10s, double[] MA20s, double[] MA30s) {
		double MA5, MA10, MA20, MA30, max, maxDiff, min, sum, denseness;
		MA5 = MA5s[MA5s.length - 1];
		MA10 = MA10s[MA10s.length - 1];
		MA20 = MA20s[MA20s.length - 1];
		MA30 = MA30s[MA30s.length - 1];

		double[] maxSeries = { MA5, MA10, MA20, MA30 };

		Arrays.sort(maxSeries);
		max = maxSeries[maxSeries.length - 1];
		min = maxSeries[0];

		maxDiff = max - min;
		sum = max + min;
		denseness = maxDiff / sum;
		return denseness;
	}

	public static double computeChipDensenessTrend(double[] MA5s, double[] MA10s, double[] MA20s, double[] MA30s,
			int span) {
		double max, maxDiff, min, sum, denseness[];
		denseness = new double[span];
		for (int i = 1; i <= span; i++) {
			double[] series = { MA5s[MA5s.length - i], MA10s[MA10s.length - i], MA20s[MA20s.length - i],
					MA30s[MA30s.length - i] };
			Arrays.sort(series);
			max = series[series.length - 1];
			min = series[0];
			maxDiff = max - min;
			sum = max + min;
			denseness[span - i] = maxDiff / sum;
		}
		return UtilityAsset.trend(denseness)[0];
	}

	public static double computeChipDensenessWidth(double[] MA5s, double[] MA10s, double[] MA20s, double[] MA30s,
			int span) {
		double max, maxDiff, min, sum, denseness[];
		denseness = new double[span];
		for (int i = 1; i <= span; i++) {
			double[] series = { MA5s[MA5s.length - i], MA10s[MA10s.length - i], MA20s[MA20s.length - i],
					MA30s[MA30s.length - i] };
			Arrays.sort(series);
			max = series[series.length - 1];
			min = series[0];
			maxDiff = max - min;
			sum = max + min;
			denseness[span - i] = maxDiff / sum;
		}
		return UtilityAsset.mean(denseness);
	}

	public static double computeMAbyVol(String stockCode, int span) throws IOException {
		stockCode = stockCode.substring(2);
		URL url = new URL("http://api.mairui.club/hszbl/fsjy/" + stockCode + "/" + span + "m/cd5268626606b8b4ef");
		String jsonStr = IOUtils.toString(url, "UTF-8");
		String regex = "[\\}]";
		jsonStr = jsonStr.replaceAll("\"", "");
		String[] ssss = jsonStr.split(regex);
		for (int i = 0; i < ssss.length - 1; i++) {
			ssss[i] = ssss[i].substring(2);
		}
		String priceVolDetails[] = null;
		String dateString = null;
		int rangeSize = simulationDateRange.size();
		SliceAssetStatus sas;
		Map<String, String> timeDetailMap = new HashMap<String, String>();
		Map<String, SliceAssetStatus> sliceMap = new HashMap<String, SliceAssetStatus>();
		for (int j = 0; j < ssss.length - 1; j++) {
			timeDetailMap.put(ssss[j].split("o")[0], ssss[j].split("o")[1]);
		}

		double[][] MASpan = new double[span][span];
		for (int i = span; i > 0; i--) {
			String month = null;
			String dayOfMonth = null;
			if (simulationDateRange.get(rangeSize - i).getMonthValue() < 10) {
				month = "0" + simulationDateRange.get(rangeSize - i).getMonthValue();
			} else {
				month = String.valueOf(simulationDateRange.get(rangeSize - i).getMonthValue());
			}
			if (simulationDateRange.get(rangeSize - i).getDayOfMonth() < 10) {
				dayOfMonth = "0" + simulationDateRange.get(rangeSize - i).getDayOfMonth();
			} else {
				dayOfMonth = String.valueOf(simulationDateRange.get(rangeSize - i).getDayOfMonth());
			}

			dateString = simulationDateRange.get(rangeSize - i).getYear() + "-" + month + "-" + dayOfMonth;

			for (String s : timeDetailMap.keySet()) {
				sas = new SliceAssetStatus();
				sas.vol = Double.parseDouble(timeDetailMap.get(s).split(",")[4].split(":")[1]);
				sas.dollarAmount = Double.parseDouble(timeDetailMap.get(s).split(",")[5].split(":")[1]);
				sas.ts = s;
				sas.sliceAvgPrice = sas.dollarAmount / sas.vol;
				sliceMap.put(s, sas);
			}

			double dayDolloarAmount = 0;
			double dayVol = 0;
			double dayAvgPrice = 0;
			for (String s : sliceMap.keySet()) {
				System.out.println("s " + s);
				System.out.println("dateString " + dateString);
				System.out.println(s.contains(dateString));
				if (s.contains(dateString)) {
					dayDolloarAmount += sliceMap.get(s).dollarAmount;
					dayVol += sliceMap.get(s).vol;
				}
			}
			dayAvgPrice = dayDolloarAmount / dayVol;
			MASpan[span - i][0] = dayVol;
			MASpan[span - i][1] = dayDolloarAmount;
		}
		double MA;
		double vol = 0;
		double amount = 0;
		for (int j = 0; j < span; j++) {
			vol += MASpan[j][0];
			amount += MASpan[j][1];
		}
		MA = amount / (vol * 100);
		return MA;
	}

	static Map<String, float[]> prevClosePrices(java.util.Date computeDate, int MASpan) throws MalformedURLException {
		Map<String, float[]> prevCloseSumEntireMarketMap = new HashMap<String, float[]>();
		LocalDate MADate;
		int MADateIndex;
		int computeDateIndex;
		int j, i;
		float closePrice;
		float[] closePrices;
		float closePricesSum;

		List<java.util.Date> sortedDays;
		for (String key : allAssetPricesMapping.keySet()) {
			closePrices = new float[MASpan - 1];
			sortedDays = new ArrayList<java.util.Date>();
			closePrice = 0;
			closePricesSum = 0;
			for (java.util.Date d : allAssetPricesMapping.get(key).keySet()) {
				sortedDays.add(d);
			}
			Collections.sort(sortedDays);

			computeDateIndex = sortedDays.indexOf(computeDate);

			if (computeDateIndex == -1) {
				computeDateIndex = sortedDays.size() - 1 + 1;
			}

			if (!(key.startsWith("sz") || key.startsWith("sh") || key.startsWith("bj"))) {
				continue;
			}

			j = 1;
			i = 0;
			while (MASpan - j > 0) {
				if (computeDateIndex - j - i < 0) {
					break;
				}
				while (allAssetPricesMapping.get(key).get(sortedDays.get(computeDateIndex - j - i)) == null
						&& (computeDateIndex - j - i) >= 0) {
					i++;
				}
				closePrice = allAssetPricesMapping.get(key).get(sortedDays.get(computeDateIndex - j - i)).closePrice;
				closePrices[j - 1] = closePrice;
				closePricesSum += closePrice;
				j++;
			}
			prevCloseSumEntireMarketMap.put(key, closePrices);
		}
		return prevCloseSumEntireMarketMap;
	}

	public static Map<String, Map<java.util.Date, AssetStatus>> setAllAssetPricesMapping(LocalDate start, LocalDate end,
			double quadraticSparsness, String sparsnessType, String sparsnessSpan, Connection conn, Statement stmt) {
		String selectDay = "select * from daily_snapshot where 股票代码 not like ('bj%') and 交易日期 >='" + start
				+ "' and is_index!='Y';";
		String selectmaxDateInDailySnapshot = "select max(交易日期) as maxDate from daily_snapshot;";
		System.out.println("selectDay: " + selectDay);
		Map<java.util.Date, AssetStatus> tinyMap;
		allAssetPricesMapping = new HashMap<String, Map<java.util.Date, AssetStatus>>();
		String assetName;
		Asset a = new Asset();
		double[] MA5s;
		double[] MA10s, MA20s;
		double[] MA30s;
		double[] MA5Sparsness;
		double[] MA10Sparsness;
		double[] MA20Sparsness;
		double[] MA30Sparsness;

		int[] MA5Index;
		int[] MA10Index;
		int[] MA20Index;
		int[] MA30Index;
		int[] MA5SparsnessIndex;
		int[] MA10SparsnessIndex;
		int[] MA20SparsnessIndex;
		int[] MA30SparsnessIndex;

		float[] closePrices;
		float closePricesSum;
		ResultSet rs, rs_maxDate;
		maxDateInDailySnapshot = null;
		java.util.Date currentDate;
		java.text.SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		try {
			// getMAData(5, conn, stmt);
			rs_maxDate = stmt.executeQuery(selectmaxDateInDailySnapshot);
			rs = stmt.executeQuery(selectDay);
			currentDate = Date.from(LocalDate.now().atStartOfDay(ZoneId.systemDefault()).toInstant());
			// currentDate = sdf.parse("2024-08-16");
			/*
			 * try {// testing code, remove once test done
			 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! currentDate = sdf.parse("2024-08-15");//
			 * testing code, remove once test done !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! }
			 * catch (ParseException e) {// testing code, remove once test done
			 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! e.printStackTrace();// testing code,
			 * remove once test done !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! } // testing code,
			 */
			while (rs_maxDate.next()) {
				maxDateInDailySnapshot = rs_maxDate.getDate("maxDate");
			}
			while (rs.next()) {
				AssetStatus as = new AssetStatus();
				as.tradeDate = rs.getDate("交易日期");
				System.out.println("交易日期: " + as.tradeDate);
				as.stockCode = rs.getString("股票代码");
				if (as.stockCode == null) {
					continue;
				}
				// to exclude 688 assets from real time quotes.
				/*
				 * if (as.stockCode.startsWith("sh688")) { continue; }
				 */
				System.out.println("股票代码: " + as.stockCode);
				as.stockName = rs.getString("股票名称");
				System.out.println("股票名称: " + as.stockName);
				as.openPrice = rs.getFloat("开盘价");
				as.closePrice = rs.getFloat("收盘价");
				as.highPrice = rs.getFloat("最高价");
				as.lowPrice = rs.getFloat("最低价");
				as.amountShares = rs.getFloat("成交量");
				as.amountDollars = rs.getFloat("成交额");
				as.changeRate = rs.getDouble("涨跌幅");
				as.MA_5 = rs.getFloat("MA_5");
				as.MA_10 = rs.getFloat("MA_10");
				as.MA_20 = rs.getFloat("MA_20");
				as.MA_30 = rs.getFloat("MA_30");
				as.MA_60 = rs.getFloat("MA_60");
				as.totalMarketValue = rs.getDouble("总市值");
				as.priceDiff = as.closePrice - as.openPrice;
				if (allAssetPricesMapping.containsKey(as.stockCode)) {
					tinyMap = allAssetPricesMapping.get(as.stockCode);
					if (tinyMap.containsKey(as.tradeDate)) {
						// continue;
					} else {
						tinyMap.put(as.tradeDate, as);
						allAssetPricesMapping.put(as.stockCode, tinyMap);
					}
				} else {
					tinyMap = new HashMap<java.util.Date, AssetStatus>();
					tinyMap.put(as.tradeDate, as);
					allAssetPricesMapping.put(as.stockCode, tinyMap);
				}
			}
			shortToLong(conn, stmt);

			entireMarketPrevCloseMapMA5 = prevClosePrices(currentDate, 5);
			entireMarketPrevCloseMapMA10 = prevClosePrices(currentDate, 10);
			entireMarketPrevCloseMapMA20 = prevClosePrices(currentDate, 20);
			entireMarketPrevCloseMapMA30 = prevClosePrices(currentDate, 30);

			/*
			 * conn.setAutoCommit(false); PreparedStatement preparedStmt =
			 * conn.prepareStatement(
			 * "insert into realtimeprice (stock_code, trade_date, open_price, close_price, high_price, low_price, change_rate, amount_shares, amount_dollars, five_min_rise, speed) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
			 * );
			 */
			if (currentDate.after(maxDateInDailySnapshot)) {
				// Map<String, Float> priceMap;
				String selectRealtimePriceData = "select * from RealtimePrice;";
				try {
					rs = stmt.executeQuery(selectRealtimePriceData);
					while (rs.next()) {
						AssetStatus as = new AssetStatus();
						as.stockCode = rs.getString("stock_code");
						if (as.stockCode == null) {
							continue;
						}
						as.tradeDate = rs.getDate("trade_date");
						System.out.println("股票代码: " + as.stockCode);
						System.out.println("交易日期: " + as.tradeDate);
						as.openPrice = rs.getFloat("open_price");
						as.closePrice = rs.getFloat("close_price");
						as.highPrice = rs.getFloat("high_price");
						as.lowPrice = rs.getFloat("low_price");
						as.amountShares = rs.getFloat("amount_shares");
						as.amountDollars = rs.getFloat("amount_dollars");
						as.changeRate = rs.getDouble("change_rate");
						as.fiveMinRise = rs.getDouble("five_min_rise");
						as.speed = rs.getFloat("speed");
						as.totalMarketValue = rs.getDouble("total_marketValue");

						if (as.stockCode != null && entireMarketPrevCloseMapMA5.get(as.stockCode) != null) {
							if (entireMarketPrevCloseMapMA5.get(as.stockCode).length < 4) {
								as.MA_5 = -1;
							} else {
								closePrices = new float[4];
								closePricesSum = 0;
								closePrices = entireMarketPrevCloseMapMA5.get(as.stockCode);
								for (int j = 0; j < 4; j++) {
									// System.out.println(as.stockCode + ", close price: " + closePrices[j]);
									// System.out.println("j: " + j);
									closePricesSum += closePrices[j];
								}
								as.MA_5 = (closePricesSum + as.closePrice) / 5;
							}

							if (entireMarketPrevCloseMapMA10.get(as.stockCode).length < 9) {
								as.MA_10 = -1;
							} else {
								closePrices = new float[9];
								closePricesSum = 0;
								closePrices = entireMarketPrevCloseMapMA10.get(as.stockCode);
								for (int j = 0; j < 9; j++) {
									closePricesSum += closePrices[j];
								}
								as.MA_10 = (closePricesSum + as.closePrice) / 10;
							}

							if (entireMarketPrevCloseMapMA20.get(as.stockCode).length < 19) {
								as.MA_20 = -1;
							} else {
								closePrices = new float[19];
								closePricesSum = 0;
								closePrices = entireMarketPrevCloseMapMA20.get(as.stockCode);
								for (int j = 0; j < 19; j++) {
									closePricesSum += closePrices[j];
								}
								as.MA_20 = (closePricesSum + as.closePrice) / 20;
							}
							if (entireMarketPrevCloseMapMA30.get(as.stockCode).length < 29) {
								as.MA_30 = -1;
							} else {
								closePrices = new float[29];
								closePricesSum = 0;
								closePrices = entireMarketPrevCloseMapMA30.get(as.stockCode);
								for (int j = 0; j < 29; j++) {
									closePricesSum += closePrices[j];
								}
								as.MA_30 = (closePricesSum + as.closePrice) / 20;
							}
						}

						as.priceDiff = as.closePrice - as.openPrice;
						if (allAssetPricesMapping.get(as.stockCode).get(maxDateInDailySnapshot) != null) {
							as.stockName = allAssetPricesMapping.get(as.stockCode)
									.get(maxDateInDailySnapshot).stockName;
						}
						if (allAssetPricesMapping.containsKey(as.stockCode)) {
							tinyMap = allAssetPricesMapping.get(as.stockCode);
							if (tinyMap.containsKey(as.tradeDate)) {
							} else {
								tinyMap.put(as.tradeDate, as);
								allAssetPricesMapping.put(as.stockCode, tinyMap);
							}
						} else {
							tinyMap = new HashMap<java.util.Date, AssetStatus>();
							tinyMap.put(as.tradeDate, as);
							allAssetPricesMapping.put(as.stockCode, tinyMap);
						}
					}
				} catch (Exception ex) {
					ex.printStackTrace();
				}

				/*
				 * URL url = new URL("http://a.mairui.club/hsrl/ssjy/all/cd5268626606b8b4ef");
				 * String jsonStr = IOUtils.toString(url, "UTF-8"); jsonStr =
				 * jsonStr.replaceAll("\"", ""); String regex = "[\\}]"; String[] ssss =
				 * jsonStr.split(regex); for (int i = 0; i < ssss.length - 1; i++) { ssss[i] =
				 * ssss[i].substring(2); } String priceDetails[]; LocalDate ld = null; LocalTime
				 * lt = null; Map<String, Float> priceMap;
				 * 
				 * for (int i = 0; i < ssss.length - 1; i++) { AssetStatus as = new
				 * AssetStatus(); priceDetails = ssss[i].split(","); priceMap = new
				 * HashMap<String, Float>(); for (int j = 0; j < priceDetails.length; j++) { if
				 * (priceDetails[j].split(":")[0].equals("t")) { ld =
				 * LocalDate.parse(priceDetails[j].split(":")[1].substring(0, 10)); lt =
				 * LocalTime.parse(priceDetails[j].split(":")[1].substring(11) + ":" +
				 * priceDetails[j].split(":")[2] + ":" + priceDetails[j].split(":")[3]);
				 * continue; } if (priceDetails[j].split(":")[0].equals("dm")) { as.stockCode =
				 * String.valueOf(priceDetails[j].split(":")[1]); }
				 * priceMap.put(priceDetails[j].split(":")[0],
				 * Float.parseFloat(priceDetails[j].split(":")[1])); }
				 * 
				 * LocalDate currentLocalDate =
				 * currentDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
				 * ListIterator<LocalDate> li = priceDateRange.listIterator(); LocalDate
				 * cursorDate; if (!priceDateRange.contains(currentLocalDate)) { while
				 * (li.hasNext()) { cursorDate = li.next(); currentDate =
				 * Date.from(cursorDate.atStartOfDay(ZoneId.systemDefault()).toInstant()); if
				 * (cursorDate.isAfter(currentLocalDate)) { break; } } } as.tradeDate =
				 * java.sql.Date.valueOf(ld); // as.stockCode =
				 * String.valueOf(priceMap.get("dm")); as.stockCode =
				 * longForms.get(as.stockCode); System.out.println("股票代码: " + as.stockCode);
				 * System.out.println("交易日期: " + as.tradeDate); as.openPrice =
				 * priceMap.get("o"); as.closePrice = priceMap.get("p"); as.highPrice =
				 * priceMap.get("h"); as.lowPrice = priceMap.get("l"); as.amountShares =
				 * priceMap.get("v"); as.amountDollars = priceMap.get("cje"); as.changeRate =
				 * priceMap.get("pc"); as.fiveMinRise = priceMap.get("fm"); as.speed =
				 * priceMap.get("zs"); preparedStmt.setString(1, as.stockCode);
				 * preparedStmt.setDate(2, as.tradeDate); preparedStmt.setFloat(3,
				 * as.openPrice); preparedStmt.setFloat(4, as.closePrice);
				 * preparedStmt.setFloat(5, as.highPrice); preparedStmt.setFloat(6,
				 * as.lowPrice); preparedStmt.setDouble(7, as.changeRate);
				 * preparedStmt.setFloat(8, as.amountShares); preparedStmt.setFloat(9,
				 * as.amountDollars); preparedStmt.setDouble(10, as.fiveMinRise);
				 * preparedStmt.setDouble(11, as.speed); preparedStmt.addBatch();
				 * 
				 * String truncateRealtimePriceData = "truncate RealtimePrice;"; try {
				 * stmt.executeQuery(truncateRealtimePriceData); } catch (Exception ex) {
				 * ex.printStackTrace(); }
				 * 
				 * preparedStmt.executeBatch(); // Commit into real time table conn.commit();
				 * conn.setAutoCommit(true);
				 */
			}

			for (String key : allAssetPricesMapping.keySet()) {
				for (java.util.Date date : allAssetPricesMapping.get(key).keySet()) {
					// System.out.println("date: " + date);
					todayIndex = priceDateRange
							.indexOf(Instant.ofEpochMilli(date.getTime()).atZone(ZoneId.systemDefault()).toLocalDate());

					MA5Index = new int[3];
					MA10Index = new int[8];
					MA20Index = new int[15];
					MA30Index = new int[20];

					MA5s = new double[3];
					MA10s = new double[8];
					MA20s = new double[15];
					MA30s = new double[20];

					int i = 3;
					while (todayIndex - i >= 0 && i > 0) {
						MA5Index[3 - i] = todayIndex - i + 1;
						i--;
					}
					int j = 8;
					while (todayIndex - j >= 0 && j > 0) {
						MA10Index[8 - j] = todayIndex - j + 1;
						j--;
					}
					int s = 15;
					while (todayIndex - s >= 0 && s > 0) {
						MA20Index[15 - s] = todayIndex - s + 1;
						s--;
					}
					int k = 20;
					while (todayIndex - k >= 0 && k > 0) {
						MA30Index[20 - k] = todayIndex - k + 1;
						k--;
					}

					MA5SparsnessIndex = new int[30];
					MA10SparsnessIndex = new int[30];
					MA20SparsnessIndex = new int[30];
					MA30SparsnessIndex = new int[30];

					MA5Sparsness = new double[30];
					MA10Sparsness = new double[30];
					MA20Sparsness = new double[30];
					MA30Sparsness = new double[30];

					i = 30;
					while (todayIndex - i >= 0 && i > 0) {
						MA5SparsnessIndex[30 - i] = todayIndex - i + 1;
						MA10SparsnessIndex[30 - i] = todayIndex - i + 1;
						MA20SparsnessIndex[30 - i] = todayIndex - i + 1;
						MA30SparsnessIndex[30 - i] = todayIndex - i + 1;
						i--;
					}
					int innerIndexMA5 = 0;
					int innerIndexMA10 = 0;
					int innerIndexMA20 = 0;
					int innerIndexMA30 = 0;

					for (i = 0; i < 30; i++) { // start to assemble MA arrays
						if (i < 3) {
							innerIndexMA5 = i;
						}

						if (allAssetPricesMapping.get(key).get(Date.from(priceDateRange.get(MA5Index[innerIndexMA5])
								.atStartOfDay(ZoneId.systemDefault()).toInstant())) != null) {
							if (i < 3) {
								// System.out.println("i: " + i);
								// System.out.println("MA5Index[i]: " + MA5Index[i]);
								// System.out.println("allAssetPricesMapping.get(key): " +
								// allAssetPricesMapping.get(key));
								// System.out.println("Date MA5: " +
								// Date.from(priceDateRange.get(MA5Index[i])
								// .atStartOfDay(ZoneId.systemDefault()).toInstant()));
								MA5s[i] = allAssetPricesMapping.get(key)
										.get(Date.from(priceDateRange.get(MA5Index[innerIndexMA5])
												.atStartOfDay(ZoneId.systemDefault()).toInstant())).MA_5;
							}
						}

						if (allAssetPricesMapping.get(key).get(Date.from(priceDateRange.get(MA5SparsnessIndex[i])
								.atStartOfDay(ZoneId.systemDefault()).toInstant())) != null) {
							MA5Sparsness[i] = allAssetPricesMapping.get(key).get(Date.from(priceDateRange
									.get(MA5SparsnessIndex[i]).atStartOfDay(ZoneId.systemDefault()).toInstant())).MA_5;
						}

						if (i < 8) {
							innerIndexMA10 = i;
						}
						if (allAssetPricesMapping.get(key).get(Date.from(priceDateRange.get(MA10Index[innerIndexMA10])
								.atStartOfDay(ZoneId.systemDefault()).toInstant())) != null) {
							if (i < 8) {
								MA10s[i] = allAssetPricesMapping.get(key).get(Date.from(priceDateRange.get(MA10Index[i])
										.atStartOfDay(ZoneId.systemDefault()).toInstant())).MA_10;
							}
						}

						if (allAssetPricesMapping.get(key).get(Date.from(priceDateRange.get(MA10SparsnessIndex[i])
								.atStartOfDay(ZoneId.systemDefault()).toInstant())) != null) {
							MA10Sparsness[i] = allAssetPricesMapping.get(key)
									.get(Date.from(priceDateRange.get(MA10SparsnessIndex[i])
											.atStartOfDay(ZoneId.systemDefault()).toInstant())).MA_10;
						}

						if (i < 15) {
							innerIndexMA20 = i;
						}
						if (allAssetPricesMapping.get(key).get(Date.from(priceDateRange.get(MA20Index[innerIndexMA20])
								.atStartOfDay(ZoneId.systemDefault()).toInstant())) != null) {
							if (i < 15) {
								MA20s[i] = allAssetPricesMapping.get(key).get(Date.from(priceDateRange.get(MA20Index[i])
										.atStartOfDay(ZoneId.systemDefault()).toInstant())).MA_20;
							}
						}
						if (allAssetPricesMapping.get(key).get(Date.from(priceDateRange.get(MA20SparsnessIndex[i])
								.atStartOfDay(ZoneId.systemDefault()).toInstant())) != null) {
							MA20Sparsness[i] = allAssetPricesMapping.get(key)
									.get(Date.from(priceDateRange.get(MA20SparsnessIndex[i])
											.atStartOfDay(ZoneId.systemDefault()).toInstant())).MA_20;
						}

						if (i < 20) {
							innerIndexMA30 = i;
						}
						if (allAssetPricesMapping.get(key).get(Date.from(priceDateRange.get(MA30Index[innerIndexMA30])
								.atStartOfDay(ZoneId.systemDefault()).toInstant())) != null) {
							if (i < 20) {
								MA30s[i] = allAssetPricesMapping.get(key)
										.get(Date.from(priceDateRange.get(MA30Index[innerIndexMA30])
												.atStartOfDay(ZoneId.systemDefault()).toInstant())).MA_30;
							}
						}

						if (allAssetPricesMapping.get(key).get(Date.from(priceDateRange.get(MA30SparsnessIndex[i])
								.atStartOfDay(ZoneId.systemDefault()).toInstant())) != null) {
							MA30Sparsness[i] = allAssetPricesMapping.get(key)
									.get(Date.from(priceDateRange.get(MA30SparsnessIndex[i])
											.atStartOfDay(ZoneId.systemDefault()).toInstant())).MA_30;
						}
					} // finishing assembling MA arrays

					allAssetPricesMapping.get(key).get(date).MA5Trend = (float) a.trend(MA5s)[0];
					allAssetPricesMapping.get(key).get(date).MA10Trend = (float) a.trend(MA10s)[0];
					allAssetPricesMapping.get(key).get(date).MA20Trend = (float) a.trend(MA20s)[0];
					allAssetPricesMapping.get(key).get(date).MA30Trend = (float) a.trend(MA30s)[0];

					allAssetPricesMapping.get(key).get(date).sparsnessSuperShort = computeSparsness(MA5Sparsness,
							MA10Sparsness, MA20Sparsness, MA30Sparsness, sparsnessType, "superShort", false,
							allAssetPricesMapping.get(key).get(date).MA_10)[0];
					allAssetPricesMapping.get(key).get(date).sparsnessShort = computeSparsness(MA5Sparsness,
							MA10Sparsness, MA20Sparsness, MA30Sparsness, sparsnessType, "short", false,
							allAssetPricesMapping.get(key).get(date).MA_10)[0];
					allAssetPricesMapping.get(key).get(date).sparsnessMid = computeSparsness(MA5Sparsness,
							MA10Sparsness, MA20Sparsness, MA30Sparsness, sparsnessType, "mid", false,
							allAssetPricesMapping.get(key).get(date).MA_10)[0];
					allAssetPricesMapping.get(key).get(date).sparsnessLong = computeSparsness(MA5Sparsness,
							MA10Sparsness, MA20Sparsness, MA30Sparsness, sparsnessType, "long", false,
							allAssetPricesMapping.get(key).get(date).MA_10)[0];
					allAssetPricesMapping.get(key).get(date).denseness = computeChipDenseness(MA5s, MA10s, MA20s,
							MA30s);
					allAssetPricesMapping.get(key).get(date).densenessTrend = computeChipDensenessTrend(MA5Sparsness,
							MA10Sparsness, MA20Sparsness, MA30Sparsness, 3);
					allAssetPricesMapping.get(key).get(date).densenessWidth = computeChipDensenessWidth(MA5Sparsness,
							MA10Sparsness, MA20Sparsness, MA30Sparsness, 5);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
		return allAssetPricesMapping;
	}

	public static Map<String, Map<java.util.Date, AssetStatus>> setAllAssetPricesMappingBasic(LocalDate start, LocalDate end,
			Connection conn, Statement stmt) {
		String selectDay = "select * from daily_snapshot where 股票代码 not like ('bj%') and 交易日期 >='" + start
				+ "' and is_index!='Y';";
		String selectmaxDateInDailySnapshot = "select max(交易日期) as maxDate from daily_snapshot;";
		System.out.println("selectDay: " + selectDay);
		Map<java.util.Date, AssetStatus> tinyMap;
		allAssetPricesMapping = new HashMap<String, Map<java.util.Date, AssetStatus>>();
		String assetName;
		Asset a = new Asset();
		float[] closePrices;
		float closePricesSum;
		ResultSet rs, rs_maxDate;
		maxDateInDailySnapshot = null;
		java.util.Date currentDate;
		java.text.SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		try {
			// getMAData(5, conn, stmt);
			rs_maxDate = stmt.executeQuery(selectmaxDateInDailySnapshot);
			rs = stmt.executeQuery(selectDay);
			currentDate = Date.from(LocalDate.now().atStartOfDay(ZoneId.systemDefault()).toInstant());
			while (rs_maxDate.next()) {
				maxDateInDailySnapshot = rs_maxDate.getDate("maxDate");
			}
			while (rs.next()) {
				AssetStatus as = new AssetStatus();
				as.tradeDate = rs.getDate("交易日期");
				System.out.println("交易日期: " + as.tradeDate);
				as.stockCode = rs.getString("股票代码");
				if (as.stockCode == null) {
					continue;
				}
				// to exclude 688 assets from real time quotes.
				/*
				 * if (as.stockCode.startsWith("sh688")) { continue; }
				 */
				System.out.println("股票代码: " + as.stockCode);
				as.stockName = rs.getString("股票名称");
				System.out.println("股票名称: " + as.stockName);
				as.openPrice = rs.getFloat("开盘价");
				as.closePrice = rs.getFloat("收盘价");
				as.highPrice = rs.getFloat("最高价");
				as.lowPrice = rs.getFloat("最低价");
				as.amountShares = rs.getFloat("成交量");
				as.amountDollars = rs.getFloat("成交额");
				as.changeRate = rs.getDouble("涨跌幅");
				as.MA_5 = rs.getFloat("MA_5");
				as.MA_10 = rs.getFloat("MA_10");
				as.MA_20 = rs.getFloat("MA_20");
				as.MA_30 = rs.getFloat("MA_30");
				as.MA_60 = rs.getFloat("MA_60");
				as.totalMarketValue = rs.getDouble("总市值");
				as.priceDiff = as.closePrice - as.openPrice;
				if (allAssetPricesMapping.containsKey(as.stockCode)) {
					tinyMap = allAssetPricesMapping.get(as.stockCode);
					if (tinyMap.containsKey(as.tradeDate)) {
						// continue;
					} else {
						tinyMap.put(as.tradeDate, as);
						allAssetPricesMapping.put(as.stockCode, tinyMap);
					}
				} else {
					tinyMap = new HashMap<java.util.Date, AssetStatus>();
					tinyMap.put(as.tradeDate, as);
					allAssetPricesMapping.put(as.stockCode, tinyMap);
				}
			}
			shortToLong(conn, stmt);

			entireMarketPrevCloseMapMA5 = prevClosePrices(currentDate, 5);
			entireMarketPrevCloseMapMA10 = prevClosePrices(currentDate, 10);
			entireMarketPrevCloseMapMA20 = prevClosePrices(currentDate, 20);
			entireMarketPrevCloseMapMA30 = prevClosePrices(currentDate, 30);

			/*
			 * conn.setAutoCommit(false); PreparedStatement preparedStmt =
			 * conn.prepareStatement(
			 * "insert into realtimeprice (stock_code, trade_date, open_price, close_price, high_price, low_price, change_rate, amount_shares, amount_dollars, five_min_rise, speed) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
			 * );
			 */
			if (currentDate.after(maxDateInDailySnapshot)) {
				// Map<String, Float> priceMap;
				String selectRealtimePriceData = "select * from RealtimePrice;";
				try {
					rs = stmt.executeQuery(selectRealtimePriceData);
					while (rs.next()) {
						AssetStatus as = new AssetStatus();
						as.stockCode = rs.getString("stock_code");
						if (as.stockCode == null) {
							continue;
						}
						as.tradeDate = rs.getDate("trade_date");
						System.out.println("股票代码: " + as.stockCode);
						System.out.println("交易日期: " + as.tradeDate);
						as.openPrice = rs.getFloat("open_price");
						as.closePrice = rs.getFloat("close_price");
						as.highPrice = rs.getFloat("high_price");
						as.lowPrice = rs.getFloat("low_price");
						as.amountShares = rs.getFloat("amount_shares");
						as.amountDollars = rs.getFloat("amount_dollars");
						as.changeRate = rs.getDouble("change_rate");
						as.fiveMinRise = rs.getDouble("five_min_rise");
						as.speed = rs.getFloat("speed");
						as.totalMarketValue = rs.getDouble("total_marketValue");

						if (as.stockCode != null && entireMarketPrevCloseMapMA5.get(as.stockCode) != null) {
							if (entireMarketPrevCloseMapMA5.get(as.stockCode).length < 4) {
								as.MA_5 = -1;
							} else {
								closePrices = new float[4];
								closePricesSum = 0;
								closePrices = entireMarketPrevCloseMapMA5.get(as.stockCode);
								for (int j = 0; j < 4; j++) {
									// System.out.println(as.stockCode + ", close price: " + closePrices[j]);
									// System.out.println("j: " + j);
									closePricesSum += closePrices[j];
								}
								as.MA_5 = (closePricesSum + as.closePrice) / 5;
							}

							if (entireMarketPrevCloseMapMA10.get(as.stockCode).length < 9) {
								as.MA_10 = -1;
							} else {
								closePrices = new float[9];
								closePricesSum = 0;
								closePrices = entireMarketPrevCloseMapMA10.get(as.stockCode);
								for (int j = 0; j < 9; j++) {
									closePricesSum += closePrices[j];
								}
								as.MA_10 = (closePricesSum + as.closePrice) / 10;
							}

							if (entireMarketPrevCloseMapMA20.get(as.stockCode).length < 19) {
								as.MA_20 = -1;
							} else {
								closePrices = new float[19];
								closePricesSum = 0;
								closePrices = entireMarketPrevCloseMapMA20.get(as.stockCode);
								for (int j = 0; j < 19; j++) {
									closePricesSum += closePrices[j];
								}
								as.MA_20 = (closePricesSum + as.closePrice) / 20;
							}
							if (entireMarketPrevCloseMapMA30.get(as.stockCode).length < 29) {
								as.MA_30 = -1;
							} else {
								closePrices = new float[29];
								closePricesSum = 0;
								closePrices = entireMarketPrevCloseMapMA30.get(as.stockCode);
								for (int j = 0; j < 29; j++) {
									closePricesSum += closePrices[j];
								}
								as.MA_30 = (closePricesSum + as.closePrice) / 20;
							}
						}

						as.priceDiff = as.closePrice - as.openPrice;
						if (allAssetPricesMapping.get(as.stockCode).get(maxDateInDailySnapshot) != null) {
							as.stockName = allAssetPricesMapping.get(as.stockCode)
									.get(maxDateInDailySnapshot).stockName;
						}
						if (allAssetPricesMapping.containsKey(as.stockCode)) {
							tinyMap = allAssetPricesMapping.get(as.stockCode);
							if (tinyMap.containsKey(as.tradeDate)) {
							} else {
								tinyMap.put(as.tradeDate, as);
								allAssetPricesMapping.put(as.stockCode, tinyMap);
							}
						} else {
							tinyMap = new HashMap<java.util.Date, AssetStatus>();
							tinyMap.put(as.tradeDate, as);
							allAssetPricesMapping.put(as.stockCode, tinyMap);
						}
						
					}
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return allAssetPricesMapping;
	}

	public static void main(String args[]) throws InterruptedException {
		int ranTime = 0;
		oneTimeRun = false;
		LocalDateTime startDT;
		LocalDateTime endDT = null;
		new ThruBreaker();
		LocalDate priceStart = LocalDate.of(2024, 7, 10);
		LocalDate priceTill = LocalDate.now();
		LocalDate simulationStart = LocalDate.of(2024, 8, 10);
		LocalDate simulationTill = LocalDate.now();
		setPriceDateRange(priceStart, priceTill);
		setSimulationDateRange(simulationStart, simulationTill);
		while (true) {
			LocalTime localTimeNow = LocalTime.now();
			LocalDate localDateNow = LocalDate.now();
			if (!simulationDateRange.contains(localDateNow) || simulationDateRange.contains(localDateNow)
					&& (localTimeNow.isBefore(LocalTime.of(9, 20)) || localTimeNow.isAfter(LocalTime.of(15, 10))
							|| ((localTimeNow.isAfter(LocalTime.of(11, 35))
									&& localTimeNow.isBefore(LocalTime.of(12, 57)))))) {
				oneTimeRun = true;
			} else {
				oneTimeRun = false;
				if (timer == null) {
					timer = new Timer();
					TimerTask task = new TimerHelper();
					// oneTimeRun = false;
					timer.schedule(task, 0, 90000);
				}
			}

			if ((oneTimeRun == true && ranTime < 1) || scheduledRunAuthorization == true) {
				ranTime++;
				/*
				 * while ( // (LocalTime.now().isAfter(LocalTime.of(9, 25)) && //
				 * LocalTime.now().isBefore(LocalTime.of(11, 35))) // ||
				 * (LocalTime.now().isAfter(LocalTime.of(12, 57)) && //
				 * LocalTime.now().isBefore(LocalTime.of(16, 10))) true) {
				 */
				try {
					// Register JDBC driver
					startDT = LocalDateTime.now();
					Statement stmt = conn.createStatement();
					String filter;
					double quadraticSparsness = 0.025;
					String sparsnessType = "quadratic";
					String sparsnessSpan = "mid";
					java.util.Date todayDate, tomorrowDate, dayAfterTomorrowDate, yesterdayDate, dayBeforeYesterdayDate,
							filterDate;

					setAllAssetPricesMapping(priceStart, priceTill, quadraticSparsness, sparsnessType, sparsnessSpan,
							conn, stmt);

					int tomorrowIndex, yesterdayIndex, dayBeforeYesterdayIndex, dayAfterTomorrowIndex;
					LocalDate tomorrow, dayAfterTomorrow, yesterday, dayBeforeYesterday;
					double gain = 0, gainGarbled = 0, gainAvg, daysGain = 0, daysGainAvg;
					Map<java.util.Date, Double> gainAvgMap = new HashMap<java.util.Date, Double>();
					Map<java.util.Date, Integer> priorKeysCountMap = new HashMap<java.util.Date, Integer>();
					Map<java.util.Date, Integer> posteriorKeysCountMap = new HashMap<java.util.Date, Integer>();
					int cnt = 0;
					int cntGarbled = 0;
					// System.out.println("allAssetPricesMapping: " + allAssetPricesMapping);
					List<String> posteriorKeys, priorKeys;
					List<LocalDate> dateArray = new ArrayList<LocalDate>();
					dateArray.add(LocalDate.of(2024, 7, 8));
					float todayClose, todayOpen, yesterdayClose, tomorrowHigh, todayMA5, todayMA10, yesterdayMA5,
							filterDayMA5, filterDayMA10;
					double yesterdayChange, todayChange = 0;
					String name;
					List<LocalDate> iterationArray;
					iterationArray = dateArray;
					// String truncateQuantTable = "truncate stock_quant_select;";
					String selectSeries = "select max(series_no) maxSeries, select_date from stock_quant_select;";
					String recordings = "insert into stock_quant_select (stock_code, stock_name, select_date, next_day_average_gain, sparsness, sparsness_type, sparsness_span, select_type, prices, series_no) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
					ResultSet rs = stmt.executeQuery(selectSeries);
					// truncateQuantTableStatement.execute(truncateQuantTable);
					int maxSeriesNo, maxSeriesInDatabase = 0;
					while (rs.next()) {
						maxSeriesInDatabase = rs.getInt("maxSeries");
					}

					PreparedStatement preparedStmt = conn.prepareStatement(recordings);

					for (LocalDate everyday : simulationDateRange) {
						posteriorKeys = new ArrayList<String>();
						priorKeys = new ArrayList<String>();
						todayDate = java.util.Date.from(everyday.atStartOfDay(ZoneId.systemDefault()).toInstant());
						todayIndex = simulationDateRange.indexOf(everyday);
						// System.out.println("todayIndex : " + todayIndex);
						tomorrowIndex = todayIndex + 1;
						if (tomorrowIndex < simulationDateRange.size()) {
							tomorrow = simulationDateRange.get(tomorrowIndex);
							tomorrowDate = Date.from(tomorrow.atStartOfDay(ZoneId.systemDefault()).toInstant());
						} else {
							tomorrow = null;
							tomorrowDate = null;
						}

						dayAfterTomorrowIndex = todayIndex + 2;
						if (dayAfterTomorrowIndex < simulationDateRange.size()) {
							dayAfterTomorrow = simulationDateRange.get(dayAfterTomorrowIndex);
							dayAfterTomorrowDate = Date
									.from(dayAfterTomorrow.atStartOfDay(ZoneId.systemDefault()).toInstant());
						} else {
							dayAfterTomorrow = null;
							dayAfterTomorrowDate = null;
						}
						yesterdayIndex = todayIndex - 1;
						if (yesterdayIndex == -1) {
							continue;
						} else {
							yesterday = simulationDateRange.get(yesterdayIndex);
							yesterdayDate = Date.from(yesterday.atStartOfDay(ZoneId.systemDefault()).toInstant());
						}

						dayBeforeYesterdayIndex = todayIndex - 2;

						if (todayIndex > 5 && todayIndex < simulationDateRange.size()) {
							gain = gainGarbled = 0;
							cnt = cntGarbled = 0;
							for (String key : allAssetPricesMapping.keySet()) {
								if (allAssetPricesMapping.get(key) != null
										&& allAssetPricesMapping.get(key).get(yesterdayDate) != null
										&& allAssetPricesMapping.get(key).get(todayDate) != null
								// && allAssetPricesMapping.get(key).get(tomorrowDate) != null
								) {
									if (everyday.isBefore(LocalDate.now())) {
										name = allAssetPricesMapping.get(key).get(todayDate).stockName;
										todayClose = allAssetPricesMapping.get(key).get(todayDate).closePrice;
										todayChange = allAssetPricesMapping.get(key).get(todayDate).changeRate;
										todayOpen = allAssetPricesMapping.get(key).get(todayDate).openPrice;
										todayMA5 = allAssetPricesMapping.get(key).get(todayDate).MA_5;
										todayMA10 = allAssetPricesMapping.get(key).get(todayDate).MA_10;
										yesterdayClose = allAssetPricesMapping.get(key).get(yesterdayDate).closePrice;
										yesterdayChange = allAssetPricesMapping.get(key).get(yesterdayDate).changeRate;
										yesterdayMA5 = allAssetPricesMapping.get(key).get(yesterdayDate).MA_5;

										if (((todayClose - todayOpen >= 0
												|| allAssetPricesMapping.get(key).get(todayDate).changeRate > 0) &&
										// && allAssetPricesMapping.get(key).get(yesterdayDate).amountShares /
										// allAssetPricesMapping.get(key).get(todayDate).amountShares > 1.5
												(yesterdayMA5 > 0 && yesterdayMA5 > yesterdayClose)
												&& (todayMA5 > 0 && todayMA5 <= todayClose)
												&& (todayMA10 > 0 && todayMA10 <= todayClose)
												&& allAssetPricesMapping.get(key).get(todayDate).MA5Trend > 0
												&& allAssetPricesMapping.get(key).get(todayDate).MA10Trend > 0
												&& allAssetPricesMapping.get(key)
														.get(todayDate).sparsnessMid >= quadraticSparsness)) {
											if (allAssetPricesMapping.get(key).get(todayDate) != null
													&& allAssetPricesMapping.get(key)
															.get(todayDate).changeRate > 0.06) {
												posteriorKeys.add(key);
												if (allAssetPricesMapping.get(key).get(tomorrowDate) != null
												// && allAssetPricesMapping.get(key).get(dayAfterTomorrowDate) != null
												) {
													gain = gain
															+ allAssetPricesMapping.get(key).get(tomorrowDate).highPrice
																	// / (todayOpen * 1.03);(todayMA5));
																	/ (todayClose);
													cnt++;
												}
											}
											priorKeys.add(key);

											/*
											 * gainGarbled = gainGarbled +
											 * allAssetPricesMapping.get(key).get(tomorrowDate).highPrice // /
											 * (todayOpen * 1.03); / (todayMA5); cntGarbled++;
											 */
											preparedStmt.setString(1, key);

											preparedStmt.setString(2, name);
											preparedStmt.setDate(3, Date.valueOf(everyday));
											if (allAssetPricesMapping.get(key).get(tomorrowDate) != null
											// && allAssetPricesMapping.get(key).get(dayAfterTomorrowDate) != null
											) {
												preparedStmt.setDouble(4,
														allAssetPricesMapping.get(key).get(tomorrowDate).highPrice
																// / (todayOpen * 1.03));(todayMA5));
																/ (todayClose));
											} else {
												preparedStmt.setDouble(4, 1);
											}
											preparedStmt.setDouble(5, quadraticSparsness);
											preparedStmt.setString(6, sparsnessType);
											preparedStmt.setString(7, sparsnessSpan);
											if (posteriorKeys.contains(key)) {
												preparedStmt.setString(8, "posterior");
											} else {
												preparedStmt.setString(8, "prior");
											}

											preparedStmt.setString(9,
													"yesterdayClose: " + yesterdayClose + ", yesterdayChange: "
															+ String.format("%.2f", yesterdayChange * 100)
															+ "%, todayClose: " + todayClose + ", todayChange: "
															+ String.format("%.2f", todayChange * 100) + "%, todayMA5: "
															+ todayMA5 + ", above MA5:"
															+ String.format("%.2f", (todayClose / todayMA5 - 1) * 100)
															+ "%");

											preparedStmt.setInt(10, maxSeriesInDatabase + 1);
											preparedStmt.execute();
										}
									} else {
										name = allAssetPricesMapping.get(key).get(yesterdayDate).stockName;
										filter = "realtimeMAsAndTrends";
										if (filter.equals("realtimeMAsAndTrends")) {
											filterDate = todayDate;
										} else if (filter.equals("nonRealtimeMAsAndTrends - use yeasterday's")) {
											filterDate = yesterdayDate;
										} else {
											filterDate = todayDate;
										}
//							System.out.println("key: "+key);
//							System.out.println("allAssetPricesMapping.get(key).get(todayDate).MA5: " + allAssetPricesMapping.get(key).get(todayDate).MA_5);
//							System.out.println("allAssetPricesMapping.get(key).get(todayDate).MA10: " + allAssetPricesMapping.get(key).get(todayDate).MA_10);
//							System.out.println("allAssetPricesMapping.get(key).get(todayDate).closePrice: " +allAssetPricesMapping.get(key).get(todayDate).closePrice);
//							System.out.println("allAssetPricesMapping.get(key).get(filterDate).MA5Trend: " +allAssetPricesMapping.get(key).get(filterDate).MA5Trend);
//							System.out.println("allAssetPricesMapping.get(key).get(filterDate).MA10Trend: " + allAssetPricesMapping.get(key).get(filterDate).MA10Trend);
//							System.out.println("allAssetPricesMapping.get(key).get(filterDate).sparseness: " + allAssetPricesMapping.get(key).get(filterDate).sparsness);
										// + allAssetPricesMapping.get(key).get(tomorrowDate).highPrice
										// / allAssetPricesMapping.get(key).get(todayDate).closePrice);
										todayClose = allAssetPricesMapping.get(key).get(todayDate).closePrice;
										todayChange = allAssetPricesMapping.get(key).get(todayDate).changeRate;
										todayOpen = allAssetPricesMapping.get(key).get(todayDate).openPrice;
										if (allAssetPricesMapping.get(key).get(tomorrowDate) != null)
											tomorrowHigh = allAssetPricesMapping.get(key).get(tomorrowDate).highPrice;
										todayMA5 = allAssetPricesMapping.get(key).get(todayDate).MA_5;
										todayMA10 = allAssetPricesMapping.get(key).get(todayDate).MA_10;
										yesterdayClose = allAssetPricesMapping.get(key).get(yesterdayDate).closePrice;
										yesterdayChange = allAssetPricesMapping.get(key).get(yesterdayDate).changeRate;
										yesterdayMA5 = allAssetPricesMapping.get(key).get(yesterdayDate).MA_5;
										filterDayMA5 = allAssetPricesMapping.get(key).get(filterDate).MA_5;
										filterDayMA10 = allAssetPricesMapping.get(key).get(filterDate).MA_10;
										if ((todayClose - todayOpen >= 0
												|| allAssetPricesMapping.get(key).get(todayDate).changeRate > 0) &&
										// allAssetPricesMapping.get(key).get(yesterdayDate).amountShares
										// / allAssetPricesMapping.get(key).get(todayDate).amountShares > 1.5
												(yesterdayMA5 > 0 && yesterdayMA5 > yesterdayClose)
												&& (filterDayMA5 > 0 && filterDayMA5 <= todayClose)
												&& (filterDayMA10 > 0 && filterDayMA10 <= todayClose)
												&& allAssetPricesMapping.get(key).get(filterDate).MA5Trend > 0
												&& allAssetPricesMapping.get(key).get(filterDate).MA10Trend > 0
												&& allAssetPricesMapping.get(key)
														.get(todayDate).sparsnessMid >= quadraticSparsness) {

											if (allAssetPricesMapping.get(key).get(todayDate) != null
													&& allAssetPricesMapping.get(key).get(todayDate).changeRate > 0) {
												posteriorKeys.add(key);
												if (allAssetPricesMapping.get(key).get(tomorrowDate) != null
												// && allAssetPricesMapping.get(key).get(dayAfterTomorrowDate) != null
												) {
													// System.out.println("next day: " + tomorrowDate);
													// System.out.println("next day high: "
													// + allAssetPricesMapping.get(key).get(tomorrowDate).highPrice);
													// System.out.println("next day gain: "
													// + allAssetPricesMapping.get(key).get(tomorrowDate).highPrice
													// / allAssetPricesMapping.get(key).get(todayDate).closePrice);
													// gain = gain +
													// allAssetPricesMapping.get(key).get(tomorrowDate).highPrice
													// / allAssetPricesMapping.get(key).get(todayDate).closePrice;
													gain = gain
															+ allAssetPricesMapping.get(key).get(tomorrowDate).highPrice
																	/ (todayOpen * 1.03);
													cnt++;
												}
											}
											priorKeys.add(key);
											preparedStmt.setString(1, key);
											preparedStmt.setString(2, name);
											preparedStmt.setDate(3, Date.valueOf(LocalDate.now()));
											preparedStmt.setDouble(4, 1);
											preparedStmt.setDouble(5, quadraticSparsness);
											preparedStmt.setString(6, sparsnessType);
											preparedStmt.setString(7, sparsnessSpan);
											if (posteriorKeys.contains(key)) {
												preparedStmt.setString(8, "posterior");
											} else {
												preparedStmt.setString(8, "prior");
											}

											preparedStmt.setString(9,
													"yesterdayClose: " + yesterdayClose + ", yesterdayChange: "
															+ String.format("%.2f", yesterdayChange * 100)
															+ "%, todayClose: " + todayClose + ", todayChange: "
															+ String.format("%.2f", todayChange) + "%, todayMA5: "
															+ todayMA5 + ", above MA5:"
															+ String.format("%.2f", (todayClose / todayMA5 - 1) * 100)
															+ "%");
											preparedStmt.setInt(10, maxSeriesInDatabase + 1);
											preparedStmt.execute();
										}
									}
								}
							}
						}
						if (posteriorKeys.size() == 0) {
							System.out.println(
									"no competent posterior stock(s) found on  " + todayDate + " today's gain is -1");
						} else {
							if (gain != 0) {
								gainAvg = gain / cnt;
								System.out.println(
										todayDate + ", today's posterior selection average gain is: " + gainAvg);
							} else {
								gainAvg = 1;
							}
							System.out.println("avg: " + gainAvg);
							gainAvgMap.put(todayDate, gainAvg);
							posteriorKeysCountMap.put(todayDate, posteriorKeys.size());
						}

						if (priorKeys.size() == 0) {
							System.out.println(
									"no competent prior stock(s) found on  " + todayDate + " today's gain is -1");
						} else {
							priorKeysCountMap.put(todayDate, priorKeys.size());
						}

						System.out.println("on: " + everyday + ", " + priorKeys.size() + " prior stocks(s) picked:");
						for (String k : priorKeys) {
							System.out.println("\"" + k + "\",");
						}

						System.out.println(
								"on: " + everyday + ", " + posteriorKeys.size() + " posterior stocks(s) picked:");
						for (String k : posteriorKeys) {
							System.out.println("\"" + k + "\",");
						}
					}

					List<java.util.Date> sortedDays = new ArrayList<java.util.Date>();
					for (java.util.Date d : gainAvgMap.keySet()) {
						sortedDays.add(d);
					}

					Collections.sort(sortedDays);
					for (java.util.Date d : sortedDays) {
						System.out.println("on " + d + ", gainList avg: " + gainAvgMap.get(d) + ".");
					}

					System.out.println("gain averages: ");
					for (java.util.Date d : sortedDays) {
						System.out.println(gainAvgMap.get(d));
					}

					System.out.println("prior Keys daily counts: ");
					for (java.util.Date d : sortedDays) {
						System.out.println("on " + d + ", " + priorKeysCountMap.get(d));

					}

					System.out.println("posterior Keys daily counts: ");
					for (java.util.Date d : sortedDays) {
						System.out.println("on " + d + ", " + posteriorKeysCountMap.get(d));

					}

					double quantTraingGains = 1.0;
					for (java.util.Date d : sortedDays) {
						quantTraingGains = quantTraingGains * gainAvgMap.get(d);
					}
					boolean runDensenessDownward = true;

					if (runDensenessDownward == false) {
						System.out.println("quant trading MA upImpaler Gains since: " + simulationStart + ", "
								+ quadraticSparsness + ", " + quantTraingGains);
					} else {
						System.out.println("quant trading(MA denseness) start: ");
						double densenessFound, densenessTrend, densenessWidth, MA5Trend, todayMA5price, todayMA10price,
								todayMA60price, todayMA20price, todayMA30price, MA10Trend, sparsness[],
								densenessThreshold = 0.01, superShortDensenessThreshold = 0.02,
								forwardSparsenessThreshold = 0.015, forwardMA5TrendThreshold = -0.01, forwardMA5Trend,
								forwardSparseness, midBackwardsSparsness, shortBackwardsSparsness,
								superShortBackwardsSparsness, longBackwardsSparsness,
								longBackwardsSparsnessThreshold = 0.01, superShortBackwardsSparsnessThreshold = 0.05,
								midBackwardsSparsnessThreshold = 0.015, densenessWidthThreshold = 0.05,
								densenessTrendThreshold = 0.006;
						// String name;
						List<Float> closePrices;
						List<Double> forwardMA5s;
						Map<LocalDate, List<String>> denseCandidates = new TreeMap<LocalDate, List<String>>();
						Map<LocalDate, List<String>> downwardCandidates = new TreeMap<LocalDate, List<String>>();
						String truncateDenseness = "truncate denseness;";
						String truncateDownward = "truncate downward;";
						try {
							stmt.executeQuery(truncateDenseness);
							stmt.executeQuery(truncateDownward);
						} catch (Exception ex) {
							ex.printStackTrace();
						}
						List<String> denseStockList, downwardStockList;
						boolean forwardSparse, upwardTrend = false, downwardTrend = false;
						for (LocalDate everyday : simulationDateRange) {
							denseStockList = new LinkedList<String>();
							downwardStockList = new LinkedList<String>();
							int todayIndex = simulationDateRange.indexOf(everyday);
							float max, min;
							Set<String> keys = allAssetPricesMapping.keySet();
							for (String key : keys) {
								if (key != null && allAssetPricesMapping.get(key).get(java.util.Date
										.from(everyday.atStartOfDay(ZoneId.systemDefault()).toInstant())) != null) {
									name = allAssetPricesMapping.get(key).get(java.util.Date
											.from(everyday.atStartOfDay(ZoneId.systemDefault()).toInstant())).stockName;
									densenessFound = allAssetPricesMapping.get(key).get(java.util.Date
											.from(everyday.atStartOfDay(ZoneId.systemDefault()).toInstant())).denseness;
									densenessTrend = allAssetPricesMapping.get(key).get(java.util.Date.from(
											everyday.atStartOfDay(ZoneId.systemDefault()).toInstant())).densenessTrend;
									densenessWidth = allAssetPricesMapping.get(key).get(java.util.Date.from(
											everyday.atStartOfDay(ZoneId.systemDefault()).toInstant())).densenessWidth;
									MA5Trend = allAssetPricesMapping.get(key).get(java.util.Date
											.from(everyday.atStartOfDay(ZoneId.systemDefault()).toInstant())).MA5Trend;
									MA10Trend = allAssetPricesMapping.get(key).get(java.util.Date
											.from(everyday.atStartOfDay(ZoneId.systemDefault()).toInstant())).MA10Trend;
									longBackwardsSparsness = allAssetPricesMapping.get(key).get(java.util.Date.from(
											everyday.atStartOfDay(ZoneId.systemDefault()).toInstant())).sparsnessLong;
									midBackwardsSparsness = allAssetPricesMapping.get(key).get(java.util.Date.from(
											everyday.atStartOfDay(ZoneId.systemDefault()).toInstant())).sparsnessMid;
									shortBackwardsSparsness = allAssetPricesMapping.get(key).get(java.util.Date.from(
											everyday.atStartOfDay(ZoneId.systemDefault()).toInstant())).sparsnessShort;
									superShortBackwardsSparsness = allAssetPricesMapping.get(key)
											.get(java.util.Date.from(everyday.atStartOfDay(ZoneId.systemDefault())
													.toInstant())).sparsnessSuperShort;
									todayOpen = allAssetPricesMapping.get(key).get(java.util.Date
											.from(everyday.atStartOfDay(ZoneId.systemDefault()).toInstant())).openPrice;
									todayClose = allAssetPricesMapping.get(key).get(java.util.Date.from(
											everyday.atStartOfDay(ZoneId.systemDefault()).toInstant())).closePrice;
									todayChange = allAssetPricesMapping.get(key).get(java.util.Date.from(
											everyday.atStartOfDay(ZoneId.systemDefault()).toInstant())).changeRate;
									todayMA5price = allAssetPricesMapping.get(key).get(java.util.Date
											.from(everyday.atStartOfDay(ZoneId.systemDefault()).toInstant())).MA_5;
									todayMA10price = allAssetPricesMapping.get(key).get(java.util.Date
											.from(everyday.atStartOfDay(ZoneId.systemDefault()).toInstant())).MA_10;
									todayMA20price = allAssetPricesMapping.get(key).get(java.util.Date
											.from(everyday.atStartOfDay(ZoneId.systemDefault()).toInstant())).MA_20;
									todayMA30price = allAssetPricesMapping.get(key).get(java.util.Date
											.from(everyday.atStartOfDay(ZoneId.systemDefault()).toInstant())).MA_30;
									todayMA60price = allAssetPricesMapping.get(key).get(java.util.Date
											.from(everyday.atStartOfDay(ZoneId.systemDefault()).toInstant())).MA_60;
									sparsness = new double[6];
									forwardSparse = false;
									forwardSparseness = 0;
									for (int i = 0; i < 4; i++) {
										if (todayIndex + i < simulationDateRange.size()
												&& allAssetPricesMapping.get(key)
														.get(java.util.Date.from(simulationDateRange.get(todayIndex + i)
																.atStartOfDay(ZoneId.systemDefault())
																.toInstant())) == null) {
											break;
										} else if (todayIndex + i < simulationDateRange.size()) {
											sparsness[i] = allAssetPricesMapping.get(key)
													.get(java.util.Date.from(simulationDateRange.get(todayIndex + i)
															.atStartOfDay(ZoneId.systemDefault())
															.toInstant())).sparsnessShort;

											if ((sparsness[i]) > forwardSparsenessThreshold) {
												forwardSparseness = sparsness[i];
												forwardSparse = true;
												break;
											}
										}
									}
									forwardMA5s = new ArrayList<Double>();
									forwardMA5Trend = 0;
									upwardTrend = false;
									downwardTrend = false;
									for (int i = 0; i < 5; i++) {
										if (todayIndex + i < simulationDateRange.size()
												&& allAssetPricesMapping.get(key)
														.get(java.util.Date.from(simulationDateRange.get(todayIndex + i)
																.atStartOfDay(ZoneId.systemDefault())
																.toInstant())) == null) {
											break;
										} else if (todayIndex + i < simulationDateRange.size()) {

											forwardMA5s.add((double) allAssetPricesMapping.get(key)
													.get(java.util.Date.from(simulationDateRange.get(todayIndex + i)
															.atStartOfDay(ZoneId.systemDefault()).toInstant())).MA_5);
											forwardMA5Trend = 0;
											Double[] d = new Double[forwardMA5s.size()];
											forwardMA5s.toArray(d);
											forwardMA5Trend = UtilityAsset.trend(d)[0];
											if (forwardMA5Trend > forwardMA5TrendThreshold) {
												upwardTrend = true;
												break;
											}
										}
										if (MA10Trend < 0) {
											downwardTrend = true;
											break;
										}
									}
									float todayClosePrice = allAssetPricesMapping.get(key)
											.get(java.util.Date.from(simulationDateRange.get(todayIndex)
													.atStartOfDay(ZoneId.systemDefault()).toInstant())).closePrice;
									double todayDenseness = allAssetPricesMapping.get(key)
											.get(java.util.Date.from(simulationDateRange.get(todayIndex)
													.atStartOfDay(ZoneId.systemDefault()).toInstant())).denseness;
									// forwardSparse == true && upwardTrend == true && downwardTrend == true &&
									if (densenessFound < densenessThreshold && densenessFound > 0
									// && forwardSparse == true
											&& longBackwardsSparsness <= longBackwardsSparsnessThreshold) {
										closePrices = new LinkedList<Float>();
										max = min = 0;
										for (int i = 0; todayIndex + i < simulationDateRange.size(); i++) {
											// System.out.println("todayIndex: "+ todayIndex+"; "+
											// simulationDateRange.size());

											if (allAssetPricesMapping.get(key)
													.get(java.util.Date.from(simulationDateRange.get(todayIndex + i)
															.atStartOfDay(ZoneId.systemDefault())
															.toInstant())) == null) {
												break;
											}
											closePrices.add(allAssetPricesMapping.get(key)
													.get(java.util.Date.from(simulationDateRange.get(todayIndex + i)
															.atStartOfDay(ZoneId.systemDefault())
															.toInstant())).closePrice);
										}
										// System.out.println("closePrices: "+closePrices.toString());
										Collections.sort(closePrices);
										max = Collections.max(closePrices);
										min = Collections.min(closePrices);

										String description = key + "(" + name + ") got long sparseness DENSE("
												+ String.format("%.4f", longBackwardsSparsness)
												+ ") and forwardSparseness(" + String.format("%.4f", forwardSparseness)
												+ ") and upwardTrend(" + String.format("%.4f", forwardMA5Trend)
												+ ") on " + everyday + " with max = " + max + "("
												+ String.format("%.2f", (max / todayClosePrice - 1) * 100) + "%)"
												+ " and min = " + min + "("
												+ String.format("%.2f", (min / todayClosePrice - 1) * 100)
												+ "%), Diff = "
												+ String.format("%.2f", ((max - min) / todayClosePrice) * 100) + "%";

										System.out.println(description);
										denseStockList.add(key + ": " + description);
										String recordDenseStock = "insert into denseness (stock_code, type, select_date, denseness, backwards_sparsness, open, close, description, name) values (?, ?, ?, ?, ?, ?, ?, ?, ?)";
										preparedStmt = conn.prepareStatement(recordDenseStock);
										preparedStmt.setString(1, key);
										preparedStmt.setString(2, "long");
										preparedStmt.setDate(3, java.sql.Date.valueOf(everyday));
										preparedStmt.setDouble(4, densenessFound);
										preparedStmt.setDouble(5, longBackwardsSparsness);
										preparedStmt.setDouble(6, todayOpen);
										preparedStmt.setDouble(7, todayClose);
										preparedStmt.setString(8, description);
										preparedStmt.setString(9, name);
										preparedStmt.execute();
									}

									if (densenessFound > 0
											// && forwardSparse == true
											// && superShortBackwardsSparsness <= superShortBackwardsSparsnessThreshold)
											&& midBackwardsSparsness <= midBackwardsSparsnessThreshold) {
										closePrices = new LinkedList<Float>();
										max = min = 0;
										for (int i = 0; todayIndex + i < simulationDateRange.size(); i++) {
											// System.out.println("todayIndex: "+ todayIndex+"; "+
											// simulationDateRange.size());

											if (allAssetPricesMapping.get(key)
													.get(java.util.Date.from(simulationDateRange.get(todayIndex + i)
															.atStartOfDay(ZoneId.systemDefault())
															.toInstant())) == null) {
												break;
											}
											closePrices.add(allAssetPricesMapping.get(key)
													.get(java.util.Date.from(simulationDateRange.get(todayIndex + i)
															.atStartOfDay(ZoneId.systemDefault())
															.toInstant())).closePrice);
										}
										// System.out.println("closePrices: "+closePrices.toString());
										Collections.sort(closePrices);
										max = Collections.max(closePrices);
										min = Collections.min(closePrices);

										String description = key + "(" + name + ") got mid sparseness DENSE("
												+ String.format("%.4f", midBackwardsSparsness)
												+ ") and forwardSparseness(" + String.format("%.4f", forwardSparseness)
												+ ") and upwardTrend(" + String.format("%.4f", forwardMA5Trend)
												+ ") on " + everyday + " with max = " + max + "("
												+ String.format("%.2f", (max / todayClosePrice - 1) * 100) + "%)"
												+ " and min = " + min + "("
												+ String.format("%.2f", (min / todayClosePrice - 1) * 100)
												+ "%), Diff = "
												+ String.format("%.2f", ((max - min) / todayClosePrice) * 100) + "%";

										System.out.println(description);
										denseStockList.add(key + ": " + description);
										String recordDenseStock = "insert into denseness (stock_code, type, select_date, denseness, backwards_sparsness, open, close, description, name) values (?, ?, ?, ?, ?, ?, ?, ?, ?)";
										preparedStmt = conn.prepareStatement(recordDenseStock);
										preparedStmt.setString(1, key);
										preparedStmt.setString(2, "mid");
										preparedStmt.setDate(3, java.sql.Date.valueOf(everyday));
										preparedStmt.setDouble(4, densenessFound);
										preparedStmt.setDouble(5, midBackwardsSparsness);
										preparedStmt.setDouble(6, todayOpen);
										preparedStmt.setDouble(7, todayClose);
										preparedStmt.setString(8, description);
										preparedStmt.setString(9, name);
										preparedStmt.execute();
									}

									if (todayClose < todayMA5price && todayClose < todayMA10price
											&& todayClose < todayMA20price && todayClose < todayMA30price
											&& (densenessWidth > densenessWidthThreshold
													&& densenessTrend > densenessTrendThreshold)) {
										closePrices = new LinkedList<Float>();
										max = min = 0;
										for (int i = 0; todayIndex + i < simulationDateRange.size(); i++) {
											if (allAssetPricesMapping.get(key)
													.get(java.util.Date.from(simulationDateRange.get(todayIndex + i)
															.atStartOfDay(ZoneId.systemDefault())
															.toInstant())) == null) {
												break;
											}
											closePrices.add(allAssetPricesMapping.get(key)
													.get(java.util.Date.from(simulationDateRange.get(todayIndex + i)
															.atStartOfDay(ZoneId.systemDefault())
															.toInstant())).closePrice);
										}
										Collections.sort(closePrices);
										max = Collections.max(closePrices);
										min = Collections.min(closePrices);
										String description = key + "(" + name + ") got DOWNWARD with densenessTrend("
												+ String.format("%.4f", densenessTrend) + ") and densenessWidth("
												+ String.format("%.4f", densenessWidth) + ") on " + everyday
												+ " with max = " + max + "("
												+ String.format("%.2f", (max / todayClosePrice - 1) * 100) + "%)"
												+ " and min = " + min + "("
												+ String.format("%.2f", (min / todayClosePrice - 1) * 100)
												+ "%), Diff = "
												+ String.format("%.2f", ((max - min) / todayClosePrice) * 100) + "%";
										System.out.println(description);
										downwardStockList.add(key + ": " + description);

										String recordDenseStock = "insert into downward (stock_code, select_date, open, close, denseness_trend, description, name) values (?, ?, ?, ?, ?, ?, ?)";
										preparedStmt = conn.prepareStatement(recordDenseStock);
										preparedStmt.setString(1, key);
										preparedStmt.setDate(2, java.sql.Date.valueOf(everyday));
										preparedStmt.setDouble(3, todayOpen);
										preparedStmt.setDouble(4, todayClose);
										preparedStmt.setDouble(5, densenessTrend);
										preparedStmt.setString(6, description);
										preparedStmt.setString(7, name);
										preparedStmt.execute();
										conn.commit();
									}
								}
							}
							denseCandidates.put(everyday, denseStockList);
							downwardCandidates.put(everyday, downwardStockList);
						}
						for (LocalDate d : denseCandidates.keySet()) {
							System.out.println(denseCandidates.get(d).size() + " stocks got dense on: " + d);
							for (String stockCode : denseCandidates.get(d)) {
								System.out.println("\"" + stockCode + "\",");
							}
						}

						for (LocalDate d : downwardCandidates.keySet()) {
							System.out.println(downwardCandidates.get(d).size() + " stocks got downwards on: " + d);
							for (String stockCode : downwardCandidates.get(d)) {
								System.out.println("\"" + stockCode + "\",");
							}
						}

						Map<String, String> denseNdownwardList = new HashMap<String, String>();
						System.out.println("denseNdownward stock list: ");
						for (LocalDate d : denseCandidates.keySet()) {
							if (d.isAfter(LocalDate.of(2024, 10, 1))) {
								for (String stockCode : denseCandidates.get(d)) {
									// System.out.println(stockCode.split(":")[0].substring(2));
									denseNdownwardList.put(stockCode.split(":")[0].substring(2), "");
								}
							}
						}
						for (LocalDate d : downwardCandidates.keySet()) {
							if (d.isAfter(LocalDate.of(2024, 10, 1))) {
								for (String stockCode : downwardCandidates.get(d)) {
									// System.out.println(stockCode.split(":")[0].substring(2));
									denseNdownwardList.put(stockCode.split(":")[0].substring(2), "");
								}
							}
						}
						for (String s : denseNdownwardList.keySet()) {
							System.out.println(s);
						}
						System.out.println("quant trading end.");
					}

					conn.commit();
					System.out.println("quant trading MA upImpaler Gains since: " + simulationStart + ", "
							+ quadraticSparsness + ", " + quantTraingGains);
					System.out.println("simulationDateRange: " + simulationDateRange);
					endDT = LocalDateTime.now();
					System.out.println("simulation end time: " + endDT);
					System.out
							.println("simulation duration: " + startDT.until(endDT, ChronoUnit.SECONDS) + " seconds.");
				} catch (Exception ex) {
					ex.printStackTrace();
				} finally {
					System.out.println(endDT);
				}

				// finished running and rest for a while
				scheduledRunAuthorization = false;

			}
			if (oneTimeRun == true && ranTime >= 1) {
				break;
			}
		}
	}
}