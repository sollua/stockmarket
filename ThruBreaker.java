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
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.io.IOUtils;
import java.net.MalformedURLException;
import java.net.URL;

class AssetStatus {
	public int closeAboveBollUpHappening;
	public float closePriceTrend60;
	public float bollMidTrend3;
	public java.util.Date tradeDate;
	public java.util.Date sparseOriginatedDate;
	public String stockCode;
	public String stockName;
	public float openPrice;
	public float lastClose;
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
	public float MA5sTrendWithoutLastMA5;
	public float closePriceTrend;
	public double totalMarketValue;
	public double denseness;
	public double sparsenessTrend3;
	public double sparsenessTrend5;
	public double chipDensenessWidth;
	public double spotChipDensenessWidth;
	public double sparsenessShort;
	public double sparsenessMid;
	public double sparsenessLong;
	public double sparsenessSuperShort, sparsenessSuperLong;
	public double fiveMinRise;
	public double sd4;
	public double volatility;
	public float speed;
	public String exchange;
	public double bollUp;
	public double bollDown;
	public double bollMid;
	public boolean todayUpCapped;
	public boolean todayDownCapped;
	public int upCapped;
	public int downCapped;
	public String MAGoldCross;
	public String MACDGoldCross;
	public String KDJGoldCross;
	public String macdGoldCrossStatus;
	public boolean isSparse;
	public boolean isVald;
	public float cciMA;
	public float cciMD;
	public float cci;
	public float typicalPrice;
	public double volatility220;
	public double volatility120;
	public double volatility90;
	public double volatility60;
	public double volatility30;
	public double volatility10;
	public double volatility5;
	public double volatility3;

	public double volatility220Standardized;
	public double volatility120Standardized;
	public double volatility90Standardized;
	public double volatility60Standardized;
	public double volatility30Standardized;
	public double volatility10Standardized;
	public double volatility5Standardized;
	public double volatility3Standardized;
	public double macd_macd;
}

class BollStatus {
	String stockCode;
	String stockName;
	java.util.Date statusDate;
	Enumeration<Integer> status;
	float openPrice;
	float closePrice;
	float highPrice;
	float lowPrice;
}

public class ThruBreaker implements Serializable {
	private static final long serialVersionUID = 1L;
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
	static Map<String, double[]> entireMarketPrevCloseMap5, entireMarketPrevCloseMap10, entireMarketPrevCloseMap20,
			entireMarketPrevCloseMap30, entireMarketPrevCloseMap60, entireMarketPrevBollUpperMap5,
			entireMarketPrevBollUpperMap10, entireMarketPrevBollUpperMap20, entireMarketPrevBollUpperMap30,
			entireMarketPrevBollMidMap5, entireMarketPrevBollMidMap10, entireMarketPrevBollMidMap20,
			entireMarketPrevBollMidMap30, entireMarketPrevBollLowerMap5, entireMarketPrevBollLowerMap10,
			entireMarketPrevBollLowerMap20, entireMarketPrevBollLowerMap30;
	static Connection conn;
	static java.util.Date maxDateInDailySnapshot;
	static Map<String, Map<java.util.Date, AssetStatus>> allAssetPricesMapping;
	// static Date maxUpwardSparseSelectDateInDatabase;
	static Date updateUpwardSparseSelectDateSince;

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

	class AscendingOrder implements Comparator<Object> {
		public int compare(Object o1, Object o2) {
			AssetStatus a1 = (AssetStatus) o1;
			AssetStatus a2 = (AssetStatus) o2;
			return new Double(a1.closePrice).compareTo(new Double(a2.closePrice));
		}
	}

	public static float round(double value, int places) {
		if (places < 0)
			throw new IllegalArgumentException();
		long factor = (long) Math.pow(10, places);
		value = value * factor;
		long tmp = Math.round(value);
		return (float) tmp / factor;
	}

	static volatile boolean scheduledRunAuthorization = false;

	static boolean oneTimeRun = false;

	static Timer timer = null;
	public static Date updateAssetInfoSince;

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
		exchangeHolidays.add(LocalDate.of(2025, 01, 01));
		exchangeHolidays.add(LocalDate.of(2025, 01, 28));
		exchangeHolidays.add(LocalDate.of(2025, 01, 29));
		exchangeHolidays.add(LocalDate.of(2025, 01, 30));
		exchangeHolidays.add(LocalDate.of(2025, 01, 31));
		exchangeHolidays.add(LocalDate.of(2025, 02, 01));
		exchangeHolidays.add(LocalDate.of(2025, 02, 02));
		exchangeHolidays.add(LocalDate.of(2025, 02, 03));
		exchangeHolidays.add(LocalDate.of(2025, 02, 04));
		exchangeHolidays.add(LocalDate.of(2025, 04, 04));
		tail = start;
		// simulationDateRange.add(start);
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
		exchangeHolidays.add(LocalDate.of(2025, 01, 01));
		exchangeHolidays.add(LocalDate.of(2025, 01, 28));
		exchangeHolidays.add(LocalDate.of(2025, 01, 29));
		exchangeHolidays.add(LocalDate.of(2025, 01, 30));
		exchangeHolidays.add(LocalDate.of(2025, 01, 31));
		exchangeHolidays.add(LocalDate.of(2025, 02, 01));
		exchangeHolidays.add(LocalDate.of(2025, 02, 02));
		exchangeHolidays.add(LocalDate.of(2025, 02, 03));
		exchangeHolidays.add(LocalDate.of(2025, 02, 04));
		exchangeHolidays.add(LocalDate.of(2025, 04, 04));
		tail = priceStart;
		// priceDateRange.add(priceStart);
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

	static java.util.Date accessPriceDateRange(java.util.Date today, int offset) {
		int offsetDateIndex = priceDateRange.indexOf(today.toInstant().atZone(ZoneId.systemDefault()).toLocalDate())
				+ offset;
		if (offsetDateIndex > 0 && offsetDateIndex < priceDateRange.size()) {
			return java.util.Date.from(priceDateRange.get(offsetDateIndex).atStartOfDay(defaultZoneId).toInstant());
		} else {
			return null;
		}
	}

	static java.util.Date accessPriceDateRange(LocalDate today, int offset) throws java.lang.IndexOutOfBoundsException {
		int offsetDateIndex = priceDateRange.indexOf(today) + offset;
		if (offsetDateIndex > 0 && offsetDateIndex < priceDateRange.size()) {
			return java.util.Date.from(priceDateRange.get(offsetDateIndex).atStartOfDay(defaultZoneId).toInstant());
		} else {
			return null;
		}
	}
	
	static java.util.Date accessPriceDateRangeSqlDate(java.sql.Date today, int offset) throws java.lang.IndexOutOfBoundsException {
		int offsetDateIndex = ThruBreaker.priceDateRange.indexOf(today.toLocalDate()) + offset;
		if (offsetDateIndex > 0 && offsetDateIndex < priceDateRange.size()) {
			return java.util.Date.from(priceDateRange.get(offsetDateIndex).atStartOfDay(defaultZoneId).toInstant());
		} else {
			return null;
		}
	}
	
	static java.util.Date accessPriceDateRange(int todayIndex, int offset) {
		int offsetDateIndex = todayIndex + offset;
		if (offsetDateIndex > 0 && offsetDateIndex < priceDateRange.size()) {
			return java.util.Date.from(priceDateRange.get(offsetDateIndex).atStartOfDay(defaultZoneId).toInstant());
		} else {
			return null;
		}
	}
	
	static LocalDate accessPriceDateRangeLocalDate(LocalDate today, int offset) {
		int offsetDateIndex = todayIndex + offset;
		if (offsetDateIndex > 0) {
			return priceDateRange.get(offsetDateIndex);
		} else {
			return null;
		}
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
						"http://api.mairuiapi.com/hszbl/fsjy/" + longCode + "/" + span + "m/cd5268626606b8b4ef");
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

	public static double[] computeSparseness(double[] MA5, double[] MA10, double[] MA20, double[] MA30, String type,
			String span, boolean currentDateIncluded, float ScalingValue) {
		int spanOfEntanglement;
		double SparsenessMA5MA10, SparsenessMA10MA20 = 0;
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
		} else if (span.equals("superLong")) {
			spanOfEntanglement = 45;
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
			SparsenessMA5MA10 = UtilityAsset.trend(betweenMA5MA10, "slope");
			SparsenessMA10MA20 = UtilityAsset.trend(betweenMA10MA20, "slope");
		} else if (type.equals("quadratic")) {
			for (int i = 0; i < betweenMA5MA10.length; i++) {
				betweenMA5MA10Quadratic[i] = Math.pow(betweenMA5MA10[i], 2);
				quadraticSumMA5MA10 += betweenMA5MA10Quadratic[i];
			}
			for (int i = 0; i < betweenMA10MA20.length; i++) {
				betweenMA10MA20Quadratic[i] = Math.pow(betweenMA10MA20[i], 2);
				quadraticSumMA10MA20 += betweenMA10MA20Quadratic[i];
			}
			SparsenessMA5MA10 = Math.sqrt(quadraticSumMA5MA10 / betweenMA5MA10.length) / ScalingValue;
			SparsenessMA10MA20 = Math.sqrt(quadraticSumMA10MA20 / betweenMA10MA20.length) / ScalingValue;
		} else {
			SparsenessMA5MA10 = 1;
			SparsenessMA10MA20 = 1;
		}
		double[] sparnessArray = { SparsenessMA5MA10, SparsenessMA10MA20 };
		return sparnessArray;
	}

	public static double computeSpotChipDenseness(double[] MA5s, double[] MA10s, double[] MA20s) {
		double MA5, MA10, MA20, max, maxDiff, min, avg, denseness;
		MA5 = MA5s[MA5s.length - 1];
		MA10 = MA10s[MA10s.length - 1];
		MA20 = MA20s[MA20s.length - 1];

		double[] series = { MA5, MA10, MA20 };
		Arrays.sort(series);
		max = series[series.length - 1];
		min = series[0];
		maxDiff = max - min;
		avg = (max + min) / 2;

		if (MA5 == min) {
			denseness = -maxDiff / avg;
		} else {
			denseness = maxDiff / avg;
		}
		return denseness;
	}

	public static double computeChipDensenessWidth(double[] MA5s, double[] MA10s, double[] MA20s, int span) {
		double max, maxDiff, min, avg, chipDenseness[];
		chipDenseness = new double[span];
		for (int i = 1; i <= span; i++) {
			double[] series = { MA5s[MA5s.length - i], MA10s[MA10s.length - i], MA20s[MA20s.length - i] };
			Arrays.sort(series);
			max = series[series.length - 1];
			min = series[0];
			maxDiff = max - min;
			avg = (max + min) / 2;
			chipDenseness[span - i] = maxDiff / avg;
		}
		return UtilityAsset.mean(chipDenseness);
	}

	public static double computeChipSparsenessTrend(double[] MA5s, double[] MA10s, double[] MA20s, double[] MA30s,
			int span) {
		double max, maxDiff, min, avg, sparseness[], sparseness2[];
		sparseness = new double[span];
		sparseness2 = new double[span];
		for (int i = 1; i <= span; i++) {
			/*
			 * double[] series = { MA5s[MA5s.length - i], MA10s[MA10s.length - i],
			 * MA20s[MA20s.length - i], MA30s[MA30s.length - i]}; Arrays.sort(series); max =
			 * series[series.length - 1]; min = series[0]; maxDiff = max - min; avg = (max +
			 * min)/2; denseness[span - i] = maxDiff / avg;
			 */

			sparseness2[span - i] = 2 * (MA5s[MA5s.length - i] - MA30s[MA30s.length - span])
					/ (MA5s[MA5s.length - i] + MA30s[MA30s.length - span]);

		}
		return UtilityAsset.trend(sparseness2, "slope");
	}

	public static boolean computeValdDrucula(String key, java.util.Date date, Map<Integer, Double> criteria) {
		double max, maxDiff, min, avg, sparseness[], sparseness2[], cost;
		// sparseness = new double[span];
		// sparseness2 = new double[span];
		/*
		 * for (int i = 1; i <= span; i++) { double[] series = { MA5s[MA5s.length - i],
		 * MA10s[MA10s.length - i], MA20s[MA20s.length - i], MA30s[MA30s.length - i]};
		 * Arrays.sort(series); max = series[series.length - 1]; min = series[0];
		 * maxDiff = max - min; avg = (max + min)/2; denseness[span - i] = maxDiff /
		 * avg;
		 * 
		 * //sparseness2[span - i] = 2 * (MA5s[MA5s.length - i] - MA30s[MA30s.length -
		 * span])/ (MA5s[MA5s.length - i] + MA30s[MA30s.length - span]); ///cost =
		 * bollUpper[bollUpper.length - 1] + (bollUpper[bollUpper.length - 1] -
		 * bollMid[bollMid.length - 1]) * 0.2; }
		 */

		List<Integer> cr = new LinkedList<Integer>();
		for (int s : criteria.keySet()) {
			cr.add(s);
		}
		Collections.sort(cr);
		int dateIndex = priceDateRange
				.indexOf(Instant.ofEpochMilli(date.getTime()).atZone(ZoneId.systemDefault()).toLocalDate());
		// System.out.println("key, date: "+ key + date);
		// System.out.println(Arrays.toString(closePrices));

		for (int span : cr) {
			sparseness = new double[span];
			for (int i = 0; i < span; i++) {
				try {
					if (dateIndex - span + 1 >= 0)
						sparseness[span - i - 1] = 2
								* (allAssetPricesMapping.get(key)
										.get(java.util.Date.from(priceDateRange.get(dateIndex - i)
												.atStartOfDay(defaultZoneId).toInstant())).closePrice
										- allAssetPricesMapping.get(key)
												.get(java.util.Date.from(priceDateRange.get(dateIndex - span + 1)
														.atStartOfDay(defaultZoneId).toInstant())).closePrice)
								/ (allAssetPricesMapping.get(key)
										.get(java.util.Date.from(priceDateRange.get(dateIndex - i)
												.atStartOfDay(defaultZoneId).toInstant())).closePrice
										+ allAssetPricesMapping.get(key)
												.get(java.util.Date.from(priceDateRange.get(dateIndex - span + 1)
														.atStartOfDay(defaultZoneId).toInstant())).closePrice);
				} catch (NullPointerException n) {
					continue;
				}
			}
			if (UtilityAsset.trend(sparseness, "slope") >= criteria.get(span)) {
				return true;
			} else {
				continue;
			}
			// return UtilityAsset.trend(sparseness2, "slope");
		}
		return false;
	}

	public static int countUpCapped(AssetStatus as) throws NullPointerException {
		AssetStatus lastAs;
		int index = priceDateRange
				.indexOf(Instant.ofEpochMilli(as.tradeDate.getTime()).atZone(ZoneId.systemDefault()).toLocalDate());
		index = index - 1;
		if (index > 0) {
			System.out.println(as.stockCode);
			System.out.println(java.util.Date.from(priceDateRange.get(index).atStartOfDay(defaultZoneId).toInstant()));
			lastAs = allAssetPricesMapping.get(as.stockCode)
					.get(java.util.Date.from(priceDateRange.get(index).atStartOfDay(defaultZoneId).toInstant()));
		} else {
			lastAs = null;
		}
		if (lastAs != null && lastAs.todayUpCapped == true)
			return 1 + countUpCapped(lastAs);
		else
			return 1;
	}

	public static int countDownCapped(AssetStatus as) {
		AssetStatus lastAs;
		int index = priceDateRange
				.indexOf(Instant.ofEpochMilli(as.tradeDate.getTime()).atZone(ZoneId.systemDefault()).toLocalDate());
		index = index - 1;
		if (index > 0) {
			System.out.println(as.stockCode);
			System.out.println(java.util.Date.from(priceDateRange.get(index).atStartOfDay(defaultZoneId).toInstant()));
			lastAs = allAssetPricesMapping.get(as.stockCode)
					.get(java.util.Date.from(priceDateRange.get(index).atStartOfDay(defaultZoneId).toInstant()));
		} else {
			lastAs = null;
		}
		if (lastAs != null && lastAs.todayDownCapped == true)
			return 1 + countDownCapped(lastAs);
		else
			return 1;
	}

	public static java.util.Date traceSparseOriginatedDate(AssetStatus as) {
		AssetStatus lastAs;
		int index = priceDateRange
				.indexOf(Instant.ofEpochMilli(as.tradeDate.getTime()).atZone(ZoneId.systemDefault()).toLocalDate());
		index = index - 1;
		if (index > 0) {
			System.out.println(as.stockCode);
			System.out.println(java.util.Date.from(priceDateRange.get(index).atStartOfDay(defaultZoneId).toInstant()));
			lastAs = allAssetPricesMapping.get(as.stockCode)
					.get(java.util.Date.from(priceDateRange.get(index).atStartOfDay(defaultZoneId).toInstant()));
		} else {
			lastAs = null;
		}
		if (lastAs != null && lastAs.isSparse == true)
			return traceSparseOriginatedDate(lastAs);
		else
			return as.tradeDate;
	}

	public static double computeMAbyVol(String stockCode, int span) throws IOException {
		stockCode = stockCode.substring(2);
		URL url = new URL("http://api.mairuiapi.com/hszbl/fsjy/" + stockCode + "/" + span + "m/cd5268626606b8b4ef");
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

	static Map<String, double[]> prevClosePricesOrBoll(java.util.Date computeDate, int MASpan, String type)
			throws MalformedURLException {
		Map<String, double[]> prevCloseSumEntireMarketMap = new HashMap<String, double[]>();
		int computeDateIndex;
		int j, i;
		double value;
		double[] values;
		List<java.util.Date> sortedDays;
		for (String key : allAssetPricesMapping.keySet()) {
			values = new double[MASpan - 1];
			sortedDays = new ArrayList<java.util.Date>();
			value = 0;

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
				if (type.equals("CLOSE")) {
					value = allAssetPricesMapping.get(key).get(sortedDays.get(computeDateIndex - j - i)).closePrice;
				} else if (type.equals("BOLLUPPER")) {
					value = (float) allAssetPricesMapping.get(key).get(sortedDays.get(computeDateIndex - j - i)).bollUp;
				} else if (type.equals("BOLLMID")) {
					value = (float) allAssetPricesMapping.get(key)
							.get(sortedDays.get(computeDateIndex - j - i)).bollMid;
				} else if (type.equals("BOLLLOWER")) {
					value = (float) allAssetPricesMapping.get(key)
							.get(sortedDays.get(computeDateIndex - j - i)).bollDown;
				}
				values[j - 1] = value;
				j++;
			}
			prevCloseSumEntireMarketMap.put(key, values);
		}
		return prevCloseSumEntireMarketMap;
	}

	public static Map<String, Map<java.util.Date, AssetStatus>> setAllAssetPricesMapping(LocalDate start, LocalDate end,
			double quadraticSparseness, String SparsenessType, String SparsenessSpan, Connection conn, Statement stmt)
			throws Exception {
		String selectDay = "select * from (select 股票代码, 股票名称, CONVERT(kdj_金叉死叉  USING utf8) as kdj_gold_cross, CONVERT(MA金叉死叉 USING utf8) as MA_gold_cross,"
				+ "	CONVERT(MACD_金叉死叉 using utf8) as MACD_gold_cross, 涨跌幅, 流通市值, deleted_for_good, "
				+ "	市净率, 量比, 收盘价, 振幅,  macd_金叉死叉, 总市值, 新浪地域, 交易日期, 后复权价,  ma_5,"
				+ "	开盘价, ma_20, kdj_j, 布林线中轨, 市盈率ttm, 新浪概念, ma_60, macd_macd, 是否涨停, 布林线下轨, 最低价,  "
				+ " ma_10, 最高价, psyma, macd_dif, 连续涨停, 布林线上轨, 换手率, is_index, macd_dea, kdj_金叉死叉,"
				+ "	市现率ttm, ma金叉死叉, 新浪行业, kdj_d, ma_30,  kdj_k, 成交额, psy, rsi3, 成交量, 是否跌停, 前复权价, "
				+ "	rsi2, rsi1 from daily_snapshot where 股票代码 not like ('%bj%') and 交易日期 >='" + start +"' and is_index!='Y') s "
				+ "left join (select stock_code, select_date, type from denseness where type='upward sparse') d "
				+ "on s.交易日期 = d.select_date and s.股票代码 = d.stock_code;";
		
		selectDay = "select 股票代码, 股票名称, CONVERT(kdj_金叉死叉  USING utf8) as kdj_gold_cross, CONVERT(MA金叉死叉 USING utf8) as MA_gold_cross,"
				+ "	CONVERT(MACD_金叉死叉 using utf8) as MACD_gold_cross, 涨跌幅, 流通市值, deleted_for_good, "
				+ "	市净率, 量比, 收盘价, 振幅,  macd_金叉死叉, 总市值, 新浪地域, 交易日期, 后复权价,  ma_5,"
				+ "	开盘价, ma_20, kdj_j, 布林线中轨, 市盈率ttm, 新浪概念, ma_60, macd_macd, 是否涨停, 布林线下轨, 最低价,  "
				+ " ma_10, 最高价, psyma, macd_dif, 连续涨停, 布林线上轨, 换手率, is_index, macd_dea, kdj_金叉死叉,"
				+ "	市现率ttm, ma金叉死叉, 新浪行业, kdj_d, ma_30,  kdj_k, 成交额, psy, rsi3, 成交量, 是否跌停, 前复权价, "
				+ "	rsi2, rsi1 from daily_snapshot where 股票代码 not like ('%bj%') and 交易日期 >='" + start +"' and is_index!='Y'";

		String selectmaxDateInDailySnapshot = "select max(交易日期) as maxDate from daily_snapshot;";
		Map<java.util.Date, AssetStatus> tinyMap;
		allAssetPricesMapping = new HashMap<String, Map<java.util.Date, AssetStatus>>();
		Asset a = new Asset();
		double[] MA5s, MA10s, MA20s, MA30s;
		double[] MA5Sparseness, MA10Sparseness, MA20Sparseness, MA30Sparseness, closePricesAll, priceVolatile;

		int[] MA5Index, MA10Index, MA20Index, MA30Index;
		int[] MA5SparsenessIndex, MA10SparsenessIndex, MA20SparsenessIndex, MA30SparsenessIndex, closePricesIndex;

		Map<Integer, Double> druculaCriteria = new HashMap<Integer, Double>();
		druculaCriteria.put(new Integer(2), new Double(0.04));
		druculaCriteria.put(new Integer(3), new Double(0.02));
		druculaCriteria.put(new Integer(4), new Double(0.015));

		double[] closePrices;
		float closePricesSum;
		ResultSet rs, rs_maxDate;
		maxDateInDailySnapshot = null;
		java.util.Date currentDate;
		java.text.SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		try {
			// getMAData(5, conn, stmt);
			rs_maxDate = stmt.executeQuery(selectmaxDateInDailySnapshot);	
			currentDate = Date.from(LocalDate.now().atStartOfDay(ZoneId.systemDefault()).toInstant());
			// currentDate = sdf.parse("2024-08-16");
			while (rs_maxDate.next()) {
				maxDateInDailySnapshot = rs_maxDate.getDate("maxDate");
			}
			System.out.println("selectDay: " + selectDay);
			rs = stmt.executeQuery(selectDay);
			while (rs.next()) {
				AssetStatus as = new AssetStatus();
				as.tradeDate = rs.getDate("交易日期");
				System.out.println("交易日期: " + as.tradeDate);
				as.stockCode = rs.getString("股票代码");
				if (as.stockCode == null) {
					continue;
				}
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
				as.MAGoldCross = rs.getString("MA_gold_cross");
				as.MACDGoldCross = rs.getString("MACD_gold_cross");
				as.KDJGoldCross = rs.getString("KDJ_gold_cross");
				try {
					as.bollUp = rs.getDouble("布林线上轨");
					as.bollDown = rs.getDouble("布林线下轨");
					as.bollMid = rs.getDouble("布林线中轨");
				} catch (Exception ex) {
					ex.printStackTrace();
				}
				/*if (rs.getString("type") != null && rs.getString("type").equals("upward sparse")) {
					as.isSparse = true;
				}*/

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
			//shortToLong(conn, stmt);

//			entireMarketPrevCloseMap5 = prevClosePricesOrBoll(currentDate, 5, "CLOSE");
//			entireMarketPrevCloseMap10 = prevClosePricesOrBoll(currentDate, 10, "CLOSE");
//			entireMarketPrevCloseMap20 = prevClosePricesOrBoll(currentDate, 20, "CLOSE");
//			entireMarketPrevCloseMap30 = prevClosePricesOrBoll(currentDate, 30, "CLOSE");

			// entireMarketPrevCloseMap60 = prevClosePricesOrBoll(currentDate, 60, "CLOSE");
			/*
			 * * entireMarketPrevBollUpperMap5 = prevClosePricesOrBoll(currentDate, 5,
			 * "BOLLUPPER"); entireMarketPrevBollUpperMap10 =
			 * prevClosePricesOrBoll(currentDate, 10, "BOLLUPPER");
			 * entireMarketPrevBollUpperMap20 = prevClosePricesOrBoll(currentDate, 20,
			 * "BOLLUPPER");
			 */
			// entireMarketPrevBollUpperMap30 = prevClosePricesOrBoll(currentDate, 30,
			// "BOLLUPPER");

			/*
			 * entireMarketPrevBollMidMap5 = prevClosePricesOrBoll(currentDate, 5,
			 * "BOLLMID"); entireMarketPrevBollMidMap10 = prevClosePricesOrBoll(currentDate,
			 * 10, "BOLLMID"); entireMarketPrevBollMidMap20 =
			 * prevClosePricesOrBoll(currentDate, 20, "BOLLMID");
			 */
			//entireMarketPrevBollMidMap30 = prevClosePricesOrBoll(currentDate, 30, "BOLLMID");

			/*
			 * entireMarketPrevBollLowerMap5 = prevClosePricesOrBoll(currentDate, 5,
			 * "BOLLLOWER"); entireMarketPrevBollLowerMap10 =
			 * prevClosePricesOrBoll(currentDate, 10, "BOLLLOWER");
			 * entireMarketPrevBollLowerMap20 = prevClosePricesOrBoll(currentDate, 20,
			 * "BOLLLOWER");
			 */
			// entireMarketPrevBollLowerMap30 = prevClosePricesOrBoll(currentDate, 30,
			// "BOLLLOWER");

			if (currentDate.after(maxDateInDailySnapshot)) {
				String selectRealtimePriceData = "select * from RealtimePrice;";
				try {
					rs = stmt.executeQuery(selectRealtimePriceData);
					while (rs.next()) {
						AssetStatus as = new AssetStatus();
						as.stockCode = rs.getString("stock_code");
						if (as.stockCode == null) {
							//System.out.println("current day skipped.");
							//break;
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

/*						if (as.stockCode != null && entireMarketPrevCloseMap5.get(as.stockCode) != null) {
							if (entireMarketPrevCloseMap5.get(as.stockCode).length < 4) {
								as.MA_5 = -1;
							} else {
								closePrices = new double[4];
								closePricesSum = 0;
								closePrices = entireMarketPrevCloseMap5.get(as.stockCode);
								for (int j = 0; j < 4; j++) {
									// System.out.println(as.stockCode + ", close price: " + closePrices[j]);
									// System.out.println("j: " + j);
									closePricesSum += closePrices[j];
								}
								as.MA_5 = (closePricesSum + as.closePrice) / 5;
							}

							if (entireMarketPrevCloseMap10.get(as.stockCode).length < 9) {
								as.MA_10 = -1;
							} else {
								closePrices = new double[9];
								closePricesSum = 0;
								closePrices = entireMarketPrevCloseMap10.get(as.stockCode);
								for (int j = 0; j < 9; j++) {
									closePricesSum += closePrices[j];
								}
								as.MA_10 = (closePricesSum + as.closePrice) / 10;
							}

							if (entireMarketPrevCloseMap20.get(as.stockCode).length < 19) {
								as.MA_20 = -1;
							} else {
								closePrices = new double[19];
								closePricesSum = 0;
								closePrices = entireMarketPrevCloseMap20.get(as.stockCode);
								for (int j = 0; j < 19; j++) {
									closePricesSum += closePrices[j];
								}
								as.MA_20 = (closePricesSum + as.closePrice) / 20;
							}
							if (entireMarketPrevCloseMap30.get(as.stockCode).length < 29) {
								as.MA_30 = -1;
							} else {
								closePrices = new double[29];
								closePricesSum = 0;
								closePrices = entireMarketPrevCloseMap30.get(as.stockCode);
								for (int j = 0; j < 29; j++) {
									closePricesSum += closePrices[j];
								}
								as.MA_30 = (closePricesSum + as.closePrice) / 30;
							}
						}
*/
						as.priceDiff = as.closePrice - as.openPrice;
						try {
							if (allAssetPricesMapping.containsKey(as.stockCode)
									&& allAssetPricesMapping.get(as.stockCode).get(maxDateInDailySnapshot) != null) {
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
						} catch (Exception ex) {
							ex.printStackTrace();
							continue;
						}
					}
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}

			for (String key : allAssetPricesMapping.keySet()) {
				for (java.util.Date date : allAssetPricesMapping.get(key).keySet()) {
					// System.out.println("date: " + date);
					todayIndex = priceDateRange
							.indexOf(Instant.ofEpochMilli(date.getTime()).atZone(ZoneId.systemDefault()).toLocalDate());

					MA5Index = new int[3];
					MA10Index = new int[8];
					MA20Index = new int[15];
					MA30Index = new int[45];

					MA5s = new double[3];
					MA10s = new double[8];
					MA20s = new double[15];
					MA30s = new double[45];

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
					int k = 45;
					while (todayIndex - k >= 0 && k > 0) {
						MA30Index[45 - k] = todayIndex - k + 1;
						k--;
					}

					MA5SparsenessIndex = new int[60];
					MA10SparsenessIndex = new int[60];
					MA20SparsenessIndex = new int[60];
					MA30SparsenessIndex = new int[60];
					closePricesIndex = new int[60];

					MA5Sparseness = new double[60];
					MA10Sparseness = new double[60];
					MA20Sparseness = new double[60];
					MA30Sparseness = new double[60];
					// closePricesAll= new double[priceDateRange.size()];
					closePrices = new double[60];

					i = 60;
					while (todayIndex - i >= 0 && i > 0) {
						MA5SparsenessIndex[60 - i] = todayIndex - i + 1;
						MA10SparsenessIndex[60 - i] = todayIndex - i + 1;
						MA20SparsenessIndex[60 - i] = todayIndex - i + 1;
						MA30SparsenessIndex[60 - i] = todayIndex - i + 1;
						closePricesIndex[60 - i] = todayIndex - i + 1;
						i--;
					}
					int innerIndexMA5 = 0;
					int innerIndexMA10 = 0;
					int innerIndexMA20 = 0;
					int innerIndexMA30 = 0;

					for (i = 0; i < 60; i++) {// start to assemble MA arrays
						if (i < 3) {
							innerIndexMA5 = i;
						}

						if (allAssetPricesMapping.get(key).get(Date.from(priceDateRange.get(MA5Index[innerIndexMA5])
								.atStartOfDay(ZoneId.systemDefault()).toInstant())) != null) {
							if (i < 3) {
								/*
								 * if (allAssetPricesMapping.get(key)
								 * .get(Date.from(priceDateRange.get(MA5Index[innerIndexMA5])
								 * .atStartOfDay(ZoneId.systemDefault()).toInstant())).MA_5 == 0) { MA5s[i] =
								 * allAssetPricesMapping.get(key)
								 * .get(Date.from(priceDateRange.get(MA5Index[innerIndexMA5])
								 * .atStartOfDay(ZoneId.systemDefault()).toInstant())).closePrice; }
								 */
								MA5s[i] = allAssetPricesMapping.get(key)
										.get(Date.from(priceDateRange.get(MA5Index[innerIndexMA5])
												.atStartOfDay(ZoneId.systemDefault()).toInstant())).MA_5;
							}
						}

						if (allAssetPricesMapping.get(key).get(Date.from(priceDateRange.get(MA5SparsenessIndex[i])
								.atStartOfDay(ZoneId.systemDefault()).toInstant())) != null) {
							MA5Sparseness[i] = allAssetPricesMapping.get(key).get(Date.from(priceDateRange
									.get(MA5SparsenessIndex[i]).atStartOfDay(ZoneId.systemDefault()).toInstant())).MA_5;
						}

						if (i < 8) {
							innerIndexMA10 = i;
						}
						if (allAssetPricesMapping.get(key).get(Date.from(priceDateRange.get(MA10Index[innerIndexMA10])
								.atStartOfDay(ZoneId.systemDefault()).toInstant())) != null) {
							if (i < 8) {
								/*
								 * if (allAssetPricesMapping.get(key)
								 * .get(Date.from(priceDateRange.get(MA10Index[innerIndexMA10])
								 * .atStartOfDay(ZoneId.systemDefault()).toInstant())).MA_10 == 0) { MA10s[i] =
								 * allAssetPricesMapping.get(key)
								 * .get(Date.from(priceDateRange.get(MA10Index[innerIndexMA10])
								 * .atStartOfDay(ZoneId.systemDefault()).toInstant())).closePrice; }
								 */
								MA10s[i] = allAssetPricesMapping.get(key).get(Date.from(priceDateRange.get(MA10Index[i])
										.atStartOfDay(ZoneId.systemDefault()).toInstant())).MA_10;
							}
						}

						if (allAssetPricesMapping.get(key).get(Date.from(priceDateRange.get(MA10SparsenessIndex[i])
								.atStartOfDay(ZoneId.systemDefault()).toInstant())) != null) {
							MA10Sparseness[i] = allAssetPricesMapping.get(key)
									.get(Date.from(priceDateRange.get(MA10SparsenessIndex[i])
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
						if (allAssetPricesMapping.get(key).get(Date.from(priceDateRange.get(MA20SparsenessIndex[i])
								.atStartOfDay(ZoneId.systemDefault()).toInstant())) != null) {
							MA20Sparseness[i] = allAssetPricesMapping.get(key).get(Date.from(priceDateRange.get(MA20SparsenessIndex[i])
									.atStartOfDay(ZoneId.systemDefault()).toInstant())).MA_20;
						}

						if (i < 45) {
							innerIndexMA30 = i;
						}
						if (allAssetPricesMapping.get(key).get(Date.from(priceDateRange.get(MA30Index[innerIndexMA30])
								.atStartOfDay(ZoneId.systemDefault()).toInstant())) != null) {
							if (i < 45) {
								MA30s[i] = allAssetPricesMapping.get(key)
										.get(Date.from(priceDateRange.get(MA30Index[innerIndexMA30])
												.atStartOfDay(ZoneId.systemDefault()).toInstant())).MA_30;
							}
						}

						if (allAssetPricesMapping.get(key).get(Date.from(priceDateRange.get(MA30SparsenessIndex[i])
								.atStartOfDay(ZoneId.systemDefault()).toInstant())) != null) {
							MA30Sparseness[i] = allAssetPricesMapping.get(key)
									.get(Date.from(priceDateRange.get(MA30SparsenessIndex[i])
											.atStartOfDay(ZoneId.systemDefault()).toInstant())).MA_30;

						}
						
						if (allAssetPricesMapping.get(key).get(Date.from(priceDateRange.get(closePricesIndex[i])
								.atStartOfDay(ZoneId.systemDefault()).toInstant())) != null) {
							closePrices[i] = allAssetPricesMapping.get(key)
									.get(Date.from(priceDateRange.get(closePricesIndex[i])
											.atStartOfDay(ZoneId.systemDefault()).toInstant())).closePrice;
						}
					} // finishing assembling MA arrays

					/*
					 * { closePricesAll[i] = allAssetPricesMapping.get(key)
					 * .get(Date.from(priceDateRange.get(todayIndex + i)
					 * .atStartOfDay(ZoneId.systemDefault()).toInstant())).closePrice; }
					 */
					allAssetPricesMapping.get(key).get(date).MA5Trend = (float) a.trend(MA5s, "slope");
					allAssetPricesMapping.get(key).get(date).MA10Trend = (float) a.trend(MA10s, "slope");
					allAssetPricesMapping.get(key).get(date).MA20Trend = (float) a.trend(MA20s, "slope");
					allAssetPricesMapping.get(key).get(date).MA30Trend = (float) a
							.trend(Arrays.copyOfRange(MA30s, 35, 45), "slope");
					allAssetPricesMapping.get(key).get(date).MA5sTrendWithoutLastMA5 = (float) a
							.trend(new double[] { MA5s[0], MA5s[1] }, "slope");
					allAssetPricesMapping.get(key).get(date).closePriceTrend60 = (float) a
								.trend(closePrices, "slope");
					/*
					 * if (entireMarketPrevBollMidMap30.get(key) != null &&
					 * entireMarketPrevBollMidMap30.get(key).length >= 4) {
					 * allAssetPricesMapping.get(key).get(date).bollMidTrend3 = (float)
					 * a.trend(Arrays.copyOfRange( entireMarketPrevBollMidMap30.get(key),
					 * entireMarketPrevBollMidMap30.get(key).length - 4,
					 * entireMarketPrevBollMidMap30.get(key).length - 1), "slope"); }
					 */
					if (todayIndex - 2 >= 0
							&& allAssetPricesMapping.get(key)
									.get(Date.from(priceDateRange.get(todayIndex - 2)
											.atStartOfDay(ZoneId.systemDefault()).toInstant())) != null
							&& allAssetPricesMapping.get(key)
									.get(Date.from(priceDateRange.get(todayIndex - 1)
											.atStartOfDay(ZoneId.systemDefault()).toInstant())) != null
							&& allAssetPricesMapping.get(key).get(Date.from(priceDateRange.get(todayIndex)
									.atStartOfDay(ZoneId.systemDefault()).toInstant())) != null) {
						allAssetPricesMapping.get(key).get(date).closePriceTrend = (float) a.trend(
								new double[] {
										allAssetPricesMapping.get(key)
												.get(Date.from(priceDateRange.get(todayIndex - 2)
														.atStartOfDay(ZoneId.systemDefault()).toInstant())).closePrice,
										allAssetPricesMapping.get(key)
												.get(Date.from(priceDateRange.get(todayIndex - 1)
														.atStartOfDay(ZoneId.systemDefault()).toInstant())).closePrice,
										allAssetPricesMapping.get(key).get(Date.from(priceDateRange.get(todayIndex)
												.atStartOfDay(ZoneId.systemDefault()).toInstant())).closePrice },
								"slope");
					}

					if (todayIndex - 1 >= 0
							&& allAssetPricesMapping.get(key)
									.get(Date.from(priceDateRange.get(todayIndex - 1)
											.atStartOfDay(ZoneId.systemDefault()).toInstant())) != null
							&& allAssetPricesMapping.get(key).get(Date.from(priceDateRange.get(todayIndex)
									.atStartOfDay(ZoneId.systemDefault()).toInstant())) != null) {
						allAssetPricesMapping.get(key)
								.get(Date.from(priceDateRange.get(todayIndex).atStartOfDay(ZoneId.systemDefault())
										.toInstant())).lastClose = allAssetPricesMapping.get(key)
												.get(Date.from(priceDateRange.get(todayIndex - 1)
														.atStartOfDay(ZoneId.systemDefault()).toInstant())).closePrice;

						if ((allAssetPricesMapping.get(key).get(Date.from(priceDateRange.get(todayIndex)
								.atStartOfDay(ZoneId.systemDefault()).toInstant())).closePrice >= round(
										allAssetPricesMapping.get(key)
												.get(Date.from(priceDateRange.get(todayIndex)
														.atStartOfDay(ZoneId.systemDefault()).toInstant())).lastClose
												* 1.1,
										2) - 0.01
								&& !allAssetPricesMapping.get(key)
										.get(Date.from(priceDateRange.get(todayIndex)
												.atStartOfDay(ZoneId.systemDefault()).toInstant())).stockCode
										.startsWith("sz30"))
								|| (allAssetPricesMapping.get(key)
										.get(Date.from(priceDateRange.get(todayIndex)
												.atStartOfDay(ZoneId.systemDefault()).toInstant())).closePrice >= round(
														allAssetPricesMapping.get(key)
																.get(Date.from(priceDateRange.get(todayIndex)
																		.atStartOfDay(ZoneId.systemDefault())
																		.toInstant())).lastClose
																* 1.1,
														2) - 0.01
										&& allAssetPricesMapping.get(key)
												.get(Date.from(priceDateRange.get(todayIndex)
														.atStartOfDay(ZoneId.systemDefault()).toInstant())).stockCode
												.startsWith("sz30"))) {
							allAssetPricesMapping.get(key).get(Date.from(priceDateRange.get(todayIndex)
									.atStartOfDay(ZoneId.systemDefault()).toInstant())).todayUpCapped = true;
						}

						if ((allAssetPricesMapping.get(key).get(Date.from(priceDateRange.get(todayIndex)
								.atStartOfDay(ZoneId.systemDefault()).toInstant())).closePrice <= round(
										allAssetPricesMapping.get(key)
												.get(Date.from(priceDateRange.get(todayIndex)
														.atStartOfDay(ZoneId.systemDefault()).toInstant())).lastClose
												* 0.9,
										2) + 0.01
								&& !allAssetPricesMapping.get(key)
										.get(Date.from(priceDateRange.get(todayIndex)
												.atStartOfDay(ZoneId.systemDefault()).toInstant())).stockCode
										.startsWith("sz30"))
								|| (allAssetPricesMapping.get(key)
										.get(Date.from(priceDateRange.get(todayIndex)
												.atStartOfDay(ZoneId.systemDefault()).toInstant())).closePrice <= round(
														allAssetPricesMapping.get(key)
																.get(Date.from(priceDateRange.get(todayIndex)
																		.atStartOfDay(ZoneId.systemDefault())
																		.toInstant())).lastClose
																* 0.9,
														2) + 0.01
										&& allAssetPricesMapping.get(key)
												.get(Date.from(priceDateRange.get(todayIndex)
														.atStartOfDay(ZoneId.systemDefault()).toInstant())).stockCode
												.startsWith("sz30"))) {
							allAssetPricesMapping.get(key).get(Date.from(priceDateRange.get(todayIndex)
									.atStartOfDay(ZoneId.systemDefault()).toInstant())).todayDownCapped = true;
						}
					}

					allAssetPricesMapping.get(key).get(date).sparsenessSuperShort = computeSparseness(MA5Sparseness,
							MA10Sparseness, MA20Sparseness, MA30Sparseness, SparsenessType, "superShort", false,
							allAssetPricesMapping.get(key).get(date).MA_10)[0];
					allAssetPricesMapping.get(key).get(date).sparsenessShort = computeSparseness(MA5Sparseness,
							MA10Sparseness, MA20Sparseness, MA30Sparseness, SparsenessType, "short", false,
							allAssetPricesMapping.get(key).get(date).MA_10)[0];
					allAssetPricesMapping.get(key).get(date).sparsenessMid = computeSparseness(MA5Sparseness,
							MA10Sparseness, MA20Sparseness, MA30Sparseness, SparsenessType, "mid", false,
							allAssetPricesMapping.get(key).get(date).MA_10)[0];
					allAssetPricesMapping.get(key).get(date).sparsenessLong = computeSparseness(MA5Sparseness,
							MA10Sparseness, MA20Sparseness, MA30Sparseness, SparsenessType, "long", false,
							allAssetPricesMapping.get(key).get(date).MA_10)[0];
					allAssetPricesMapping.get(key).get(date).sparsenessSuperLong = computeSparseness(MA5Sparseness,
							MA10Sparseness, MA20Sparseness, MA30Sparseness, SparsenessType, "superLong", false,
							allAssetPricesMapping.get(key).get(date).MA_10)[0];
					allAssetPricesMapping.get(key).get(date).denseness = computeSpotChipDenseness(MA5s, MA10s, MA20s);
					allAssetPricesMapping.get(key).get(date).sparsenessTrend5 = computeChipSparsenessTrend(
							MA5Sparseness, MA10Sparseness, MA20Sparseness, MA30Sparseness, 5);
					allAssetPricesMapping.get(key).get(date).sparsenessTrend3 = computeChipSparsenessTrend(
							MA5Sparseness, MA10Sparseness, MA20Sparseness, MA30Sparseness, 3);
					// allAssetPricesMapping.get(key).get(date).isVald = computeValdDrucula(key,
					// date, druculaCriteria);
					allAssetPricesMapping.get(key).get(date).chipDensenessWidth = computeChipDensenessWidth(
							MA5Sparseness, MA10Sparseness, MA20Sparseness, 5);
					allAssetPricesMapping.get(key).get(date).spotChipDensenessWidth = computeChipDensenessWidth(
							MA5Sparseness, MA10Sparseness, MA20Sparseness, 1);
					// allAssetPricesMapping.get(key).get(date).volatility =
					// UtilityAsset.standardizedVolatility(Arrays.copyOfRange(MA5Sparseness, 0, 59))
					// UtilityAsset.queryVolatility(date, key, stmt);
				}
			}

			for (String key : allAssetPricesMapping.keySet()) {
				for (java.util.Date date : allAssetPricesMapping.get(key).keySet()) {
					try {
						if (allAssetPricesMapping.get(key).get(date).todayUpCapped == true) {
							allAssetPricesMapping.get(key).get(date).upCapped = countUpCapped(
									allAssetPricesMapping.get(key).get(date));
						} else {
							allAssetPricesMapping.get(key).get(date).upCapped = 0;
						}
						if (allAssetPricesMapping.get(key).get(date).todayDownCapped == true) {
							allAssetPricesMapping.get(key).get(date).downCapped = countDownCapped(
									allAssetPricesMapping.get(key).get(date));
						} else {
							allAssetPricesMapping.get(key).get(date).downCapped = 0;
						}
						if (allAssetPricesMapping.get(key).get(date).upCapped > 1) {
							System.out.println(key + "@" + date + " upCapped: "
									+ allAssetPricesMapping.get(key).get(date).upCapped);
						}
						/*if (allAssetPricesMapping.get(key).get(date).isSparse == true) {
							allAssetPricesMapping.get(key).get(date).sparseOriginatedDate = traceSparseOriginatedDate(
									allAssetPricesMapping.get(key).get(date));
							System.out.println(key + "@" + date + " sparse Originated Date: "
									+ allAssetPricesMapping.get(key).get(date).sparseOriginatedDate);
						} else {
							allAssetPricesMapping.get(key).get(date).sparseOriginatedDate = null;
						}*/
					} catch (Exception ex) {
						ex.printStackTrace();
					}
				}
			}

		} catch (SQLException e) {
			e.printStackTrace();}
		return allAssetPricesMapping;
	}

	public static Map<String, Map<java.util.Date, AssetStatus>> setAllAssetPricesMappingBasic(LocalDate start,
			LocalDate end, Connection conn, Statement stmt) {
		String selectDay = "select 股票代码, 股票名称, CONVERT(kdj_金叉死叉  USING utf8) as kdj_gold_cross, CONVERT(MA金叉死叉 USING utf8) as MA_gold_cross,"
				+ "	CONVERT(MACD_金叉死叉 using utf8) as MACD_gold_cross, 涨跌幅, 流通市值, deleted_for_good, "
				+ "	市净率, 量比, 收盘价, 振幅, 总市值, 新浪地域, 交易日期, 后复权价,  ma_5,"
				+ "	开盘价, ma_20, kdj_j, 布林线中轨, 市盈率ttm, 新浪概念, ma_60, macd_macd, 是否涨停, 布林线下轨, 最低价,  "
				+ " ma_10, 最高价, psyma, macd_dif, 连续涨停, 布林线上轨, 换手率, is_index, macd_dea, kdj_金叉死叉,"
				+ "	市现率ttm, ma金叉死叉, 新浪行业, kdj_d, ma_30,  kdj_k, 成交额, psy, rsi3, 成交量, 是否跌停, 前复权价, "
				+ "	rsi2, rsi1 from daily_snapshot where 股票代码 not like ('%bj%') and 交易日期 >='" + start +"' and is_index!='Y'";;
		String selectmaxDateInDailySnapshot = "select max(交易日期) as maxDate from daily_snapshot;";
		System.out.println("selectDay: " + selectDay);
		Map<java.util.Date, AssetStatus> tinyMap;
		allAssetPricesMapping = new HashMap<String, Map<java.util.Date, AssetStatus>>();
		double[] closePrices;
		float closePricesSum;
		ResultSet rs, rs_maxDate;
		maxDateInDailySnapshot = null;
		java.util.Date currentDate;

		try {
			rs_maxDate = stmt.executeQuery(selectmaxDateInDailySnapshot);
			rs = stmt.executeQuery(selectDay);
			currentDate = Date.from(LocalDate.now().atStartOfDay(ZoneId.systemDefault()).toInstant());
			while (rs_maxDate.next()) {
				maxDateInDailySnapshot = rs_maxDate.getDate("maxDate");
			}
			while (rs.next()) {
				AssetStatus as = new AssetStatus();
				as.tradeDate = new java.util.Date(rs.getDate("交易日期").getTime());
				System.out.println("交易日期: " + as.tradeDate);
				as.stockCode = rs.getString("股票代码");
				if (as.stockCode == null) {
					continue;
				}
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
				try {
					as.MACDGoldCross = rs.getString("MACD_gold_cross");
					as.macd_macd = rs.getDouble("macd_macd");
					as.bollUp = rs.getDouble("布林线上轨");
					as.bollDown = rs.getDouble("布林线下轨");
					as.bollMid = rs.getDouble("布林线中轨");
				} catch (Exception ex) {
					ex.printStackTrace();
				}
				as.totalMarketValue = rs.getDouble("总市值");
				as.priceDiff = as.closePrice - as.openPrice;
				// as.capped = rs.getInt("连续涨停");

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

			entireMarketPrevCloseMap5 = prevClosePricesOrBoll(currentDate, 5, "CLOSE");
			entireMarketPrevCloseMap10 = prevClosePricesOrBoll(currentDate, 10, "CLOSE");
			entireMarketPrevCloseMap20 = prevClosePricesOrBoll(currentDate, 20, "CLOSE");
			entireMarketPrevCloseMap30 = prevClosePricesOrBoll(currentDate, 30, "CLOSE");

			if (currentDate.after(maxDateInDailySnapshot)) {
				String selectRealtimePriceData = "select * from RealtimePrice;";
				try {
					rs = stmt.executeQuery(selectRealtimePriceData);
					while (rs.next()) {
						AssetStatus as = new AssetStatus();
						as.stockCode = rs.getString("stock_code");
						if (as.stockCode == null) {
							continue;
						}
						as.tradeDate = new java.util.Date(rs.getDate("trade_date").getTime()); ;
						System.out.println("股票代码: " + as.stockCode);
						System.out.println("交易日期: " + as.tradeDate);
						as.openPrice = rs.getFloat("open_price");
						as.closePrice = rs.getFloat("close_price");
						as.highPrice = rs.getFloat("high_price");
						as.lowPrice = rs.getFloat("low_price");
						as.amountShares = rs.getFloat("amount_shares");
						as.amountDollars = rs.getFloat("amount_dollars");
						as.changeRate = rs.getDouble("change_rate")/100;
						as.fiveMinRise = rs.getDouble("five_min_rise");
						as.speed = rs.getFloat("speed");
						as.totalMarketValue = rs.getDouble("total_marketValue");
					try {	
						java.util.Date ld= ThruBreaker.accessPriceDateRange(as.tradeDate, -1);
						if(allAssetPricesMapping.keySet().contains(as.stockCode)) {
						if (allAssetPricesMapping.get(as.stockCode).keySet().contains(ld)) {
						as.bollUp =  allAssetPricesMapping.get(as.stockCode).get(ld).bollUp;
						as.bollDown = allAssetPricesMapping.get(as.stockCode).get(ld).bollDown;
						as.bollMid = allAssetPricesMapping.get(as.stockCode).get(ld).bollMid;
						as.macd_macd =  allAssetPricesMapping.get(as.stockCode).get(ld).macd_macd;
						as.MACDGoldCross = allAssetPricesMapping.get(as.stockCode).get(ld).MACDGoldCross;
						}}
					} catch (Exception ex) {
						ex.printStackTrace();
						//continue;
					}
						if (as.stockCode != null && entireMarketPrevCloseMap5.get(as.stockCode) != null) {
							if (entireMarketPrevCloseMap5.get(as.stockCode).length < 4) {
								as.MA_5 = -1;
							} else {
								closePrices = new double[4];
								closePricesSum = 0;
								closePrices = entireMarketPrevCloseMap5.get(as.stockCode);
								for (int j = 0; j < 4; j++) {
									// System.out.println(as.stockCode + ", close price: " + closePrices[j]);
									// System.out.println("j: " + j);
									closePricesSum += closePrices[j];
								}
								as.MA_5 = (closePricesSum + as.closePrice) / 5;
							}

							if (entireMarketPrevCloseMap10.get(as.stockCode).length < 9) {
								as.MA_10 = -1;
							} else {
								closePrices = new double[9];
								closePricesSum = 0;
								closePrices = entireMarketPrevCloseMap10.get(as.stockCode);
								for (int j = 0; j < 9; j++) {
									closePricesSum += closePrices[j];
								}
								as.MA_10 = (closePricesSum + as.closePrice) / 10;
							}

							if (entireMarketPrevCloseMap20.get(as.stockCode).length < 19) {
								as.MA_20 = -1;
							} else {
								closePrices = new double[19];
								closePricesSum = 0;
								closePrices = entireMarketPrevCloseMap20.get(as.stockCode);
								for (int j = 0; j < 19; j++) {
									closePricesSum += closePrices[j];
								}
								as.MA_20 = (closePricesSum + as.closePrice) / 20;
							}
							if (entireMarketPrevCloseMap30.get(as.stockCode).length < 29) {
								as.MA_30 = -1;
							} else {
								closePrices = new double[29];
								closePricesSum = 0;
								closePrices = entireMarketPrevCloseMap30.get(as.stockCode);
								for (int j = 0; j < 29; j++) {
									closePricesSum += closePrices[j];
								}
								as.MA_30 = (closePricesSum + as.closePrice) / 20;
							}
						}

						as.priceDiff = as.closePrice - as.openPrice;
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
		ThruBreaker utilityTB = new ThruBreaker();
		LocalDate priceStart = LocalDate.of(2024, 6, 1);
		LocalDate priceTill = LocalDate.now();
		LocalDate simulationStart = LocalDate.of(2024, 9, 1);
		LocalDate simulationTill = LocalDate.now();
		setPriceDateRange(priceStart, priceTill);
		setSimulationDateRange(simulationStart, simulationTill);
		updateUpwardSparseSelectDateSince = Date.valueOf(LocalDate.parse("2024-06-01"));
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
					double quadraticSparseness = 0.025;
					String SparsenessType = "quadratic";
					String SparsenessSpan = "mid";
					java.util.Date todayDate, tomorrowDate, dayAfterTomorrowDate, yesterdayDate, dayBeforeYesterdayDate,
							filterDate;
					setAllAssetPricesMapping(priceStart, priceTill, quadraticSparseness, SparsenessType, SparsenessSpan,
							conn, stmt);
					// ThruBreaker.setAllAssetPricesMappingBasic(priceStart, LocalDate.now(),
					// ThruBreaker.conn, stmt);
					int tomorrowIndex, yesterdayIndex, dayBeforeYesterdayIndex, dayAfterTomorrowIndex;
					LocalDate tomorrow, dayAfterTomorrow, yesterday, dayBeforeYesterday;
					double gain = 0, gainGarbled = 0, gainAvg;
					Map<java.util.Date, Double> gainAvgMap = new HashMap<java.util.Date, Double>();
					Map<java.util.Date, Integer> priorKeysCountMap = new HashMap<java.util.Date, Integer>();
					Map<java.util.Date, Integer> posteriorKeysCountMap = new HashMap<java.util.Date, Integer>();
					int cnt = 0;
					int cntGarbled = 0;
					// System.out.println("allAssetPricesMapping: " + allAssetPricesMapping);
					List<String> posteriorKeys, priorKeys;
					List<LocalDate> dateArray = new ArrayList<LocalDate>();
					// dateArray.add(LocalDate.of(2024, 7, 8));
					float todayClose, todayOpen, todayHigh, todayLow, yesterdayClose, tomorrowHigh, todayMA5, todayMA10,
							yesterdayMA5, filterDayMA5, filterDayMA10;
					double yesterdayChange, todayChange = 0;
					String name;
					boolean runMAImpaler = false;
					if (runMAImpaler == true) {
						// String truncateQuantTable = "truncate stock_quant_select;";
						String recordings = "insert into stock_quant_select (stock_code, stock_name, select_date, next_day_average_gain, Sparseness, Sparseness_type, Sparseness_span, select_type, prices, series_no) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
						String selectSeries = "select max(series_no) maxSeries, select_date from stock_quant_select;";
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
											yesterdayClose = allAssetPricesMapping.get(key)
													.get(yesterdayDate).closePrice;
											yesterdayChange = allAssetPricesMapping.get(key)
													.get(yesterdayDate).changeRate;
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
															.get(todayDate).sparsenessMid >= quadraticSparseness)) {
												if (allAssetPricesMapping.get(key).get(todayDate) != null
														&& allAssetPricesMapping.get(key)
																.get(todayDate).changeRate > 0.06) {
													posteriorKeys.add(key);
													if (allAssetPricesMapping.get(key).get(tomorrowDate) != null
													// && allAssetPricesMapping.get(key).get(dayAfterTomorrowDate) !=
													// null
													) {
														gain = gain + allAssetPricesMapping.get(key)
																.get(tomorrowDate).highPrice
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
												preparedStmt.setDouble(5, quadraticSparseness);
												preparedStmt.setString(6, SparsenessType);
												preparedStmt.setString(7, SparsenessSpan);
												if (posteriorKeys.contains(key)) {
													preparedStmt.setString(8, "posterior");
												} else {
													preparedStmt.setString(8, "prior");
												}

												preparedStmt.setString(9, "yesterdayClose: " + yesterdayClose
														+ ", yesterdayChange: "
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

											todayClose = allAssetPricesMapping.get(key).get(todayDate).closePrice;
											todayChange = allAssetPricesMapping.get(key).get(todayDate).changeRate;
											todayOpen = allAssetPricesMapping.get(key).get(todayDate).openPrice;
											if (allAssetPricesMapping.get(key).get(tomorrowDate) != null)
												tomorrowHigh = allAssetPricesMapping.get(key)
														.get(tomorrowDate).highPrice;
											todayMA5 = allAssetPricesMapping.get(key).get(todayDate).MA_5;
											todayMA10 = allAssetPricesMapping.get(key).get(todayDate).MA_10;
											yesterdayClose = allAssetPricesMapping.get(key)
													.get(yesterdayDate).closePrice;
											yesterdayChange = allAssetPricesMapping.get(key)
													.get(yesterdayDate).changeRate;
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
															.get(todayDate).sparsenessMid >= quadraticSparseness) {
												if (allAssetPricesMapping.get(key).get(todayDate) != null
														&& allAssetPricesMapping.get(key)
																.get(todayDate).changeRate > 0) {
													posteriorKeys.add(key);
													if (allAssetPricesMapping.get(key).get(tomorrowDate) != null
													// && allAssetPricesMapping.get(key).get(dayAfterTomorrowDate) !=
													// null
													) {
														gain = gain + allAssetPricesMapping.get(key)
																.get(tomorrowDate).highPrice / (todayOpen * 1.03);
														cnt++;
													}
												}
												priorKeys.add(key);
												preparedStmt.setString(1, key);
												preparedStmt.setString(2, name);
												preparedStmt.setDate(3, Date.valueOf(LocalDate.now()));
												preparedStmt.setDouble(4, 1);
												preparedStmt.setDouble(5, quadraticSparseness);
												preparedStmt.setString(6, SparsenessType);
												preparedStmt.setString(7, SparsenessSpan);
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
																+ todayMA5 + ", above MA5:" + String.format("%.2f",
																		(todayClose / todayMA5 - 1) * 100)
																+ "%");
												preparedStmt.setInt(10, maxSeriesInDatabase + 1);
												preparedStmt.execute();
											}
										}
									}
								}
							}
							if (posteriorKeys.size() == 0) {
								System.out.println("no competent posterior stock(s) found on  " + todayDate
										+ " today's gain is -1");
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

							System.out
									.println("on: " + everyday + ", " + priorKeys.size() + " prior stocks(s) picked:");
							for (String k : priorKeys) {
								System.out.println("\"" + k + "\",");
							}

							System.out.println(
									"on: " + everyday + ", " + posteriorKeys.size() + " posterior stocks(s) picked:");
							for (String k : posteriorKeys) {
								System.out.println("\"" + k + "\",");
							}
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

					boolean runDensenessNDownward = true;
					if (runDensenessNDownward == false) {
						System.out.println("quant trading MA upImpaler Gains since: " + simulationStart + ", "
								+ quadraticSparseness + ", " + quantTraingGains);
					} else {
						System.out.println("quant trading MA denseness & downward start: ");
						double densenessFound, sparsenessTrend, densenessWidth, spotChipDensenessWidth, MA5Trend,
								MA5sTrendWithoutLastMA5, closePriceTrend, todayMA5price, todayMA10price, todayMA60price,
								BollLowerTrack, BollUpperTrack, BollMidTrack, todayMA20price, todayMA30price, sd4,
								MA10Trend, MA30Trend, Sparseness[], backwardMA5Trend, forwardSparseness,
								midBackwardsSparseness, shortBackwardsSparseness, superShortBackwardsSparseness,
								longBackwardsSparseness, superLongBackwardsSparseness, volatility,
								longBackwardsSparsenessThreshold = 0.03, superLongBackwardsSparsenessThreshold = 0.015,
								superShortBackwardsSparsenessThreshold = 0.05, midBackwardsSparsenessThreshold = 0.012,
								densenessWidthThreshold = 0.08, densenessTrendThreshold = -0.01,
								densenessThreshold = 0.02, superShortDensenessThreshold = 0.02,
								forwardSparsenessThreshold = 0.015, backwardMA5TrendThreshold = 0.0,
								dayChangeThreshold = 0.2, sparsenessTrendThresholdUpper = 0.03,
								MA30TrendThreshold = -0.15, sparsenessTrendThresholdLower = 0.02,
								spotChipDensenessWidthThreshold = 0.03, volatilityThreshold = 0.05, BollMidTrend3 = 0,
								closePriceTrend60, closePriceTrend60Threshold = 0.045;

						boolean runDownwardTrend = true, runMidDense = false, runSuperLong = true, runSpotChip = false,
								runMAScheme = false, runSparse = false, isVald = false, forwardSparse = true;
						String MACDGoldCross;
						// String name;
						List<Float> closePrices;
						List<Double> backwardMA5s;
						Map<LocalDate, List<String>> denseCandidates = new TreeMap<LocalDate, List<String>>();
						Map<LocalDate, List<String>> downwardCandidates = new TreeMap<LocalDate, List<String>>();
						PreparedStatement preparedStmt;
						/*
						 * String truncateDenseness = "truncate denseness;"; String truncateDownward =
						 * "truncate downward;"; try { stmt.executeQuery(truncateDenseness);
						 * stmt.executeQuery(truncateDownward); } catch (Exception ex) {
						 * ex.printStackTrace(); }
						 */
						String deleteUpwardSparseSelectDate = "delete from denseness where (type = 'upward sparse' or type ='super long') and select_date >= '"
								+ updateUpwardSparseSelectDateSince.toString() + "';";
						System.out.println(deleteUpwardSparseSelectDate);
						stmt.executeQuery(deleteUpwardSparseSelectDate);
						/*
						 * String selectmaxUpwardSparseSelectDate =
						 * "select max(select_date) maxUpwardSparseSelectDate from denseness where type = 'upward sparse';"
						 * ; ResultSet rs = stmt.executeQuery(selectmaxUpwardSparseSelectDate); //
						 * truncateQuantTableStatement.execute(truncateQuantTable); int maxSeriesNo,
						 * maxSeriesInDatabase = 0; //Date maxUpwardSparseSelectDate = null; while
						 * (rs.next()) { updateUpwardSparseSelectDateSince =
						 * rs.getDate("maxUpwardSparseSelectDate"); }
						 */
						List<String> denseStockList, downwardStockList;
						List<String> bollList;
						Map<java.util.Date, List<String>> bollMap = new HashMap<java.util.Date, List<String>>();
						for (LocalDate everyday : simulationDateRange) {
							if (!everyday.isBefore(updateUpwardSparseSelectDateSince.toLocalDate())) {
								denseStockList = new LinkedList<String>();
								downwardStockList = new LinkedList<String>();
								int todayIndex = simulationDateRange.indexOf(everyday);
								float max, min;
								Set<String> keys = allAssetPricesMapping.keySet();
								for (String key : keys) {
									if (key != null && allAssetPricesMapping.get(key).get(java.util.Date
											.from(everyday.atStartOfDay(ZoneId.systemDefault()).toInstant())) != null) {
										name = allAssetPricesMapping.get(key).get(java.util.Date.from(
												everyday.atStartOfDay(ZoneId.systemDefault()).toInstant())).stockName;
										densenessFound = allAssetPricesMapping.get(key).get(java.util.Date.from(
												everyday.atStartOfDay(ZoneId.systemDefault()).toInstant())).denseness;
										sparsenessTrend = allAssetPricesMapping.get(key)
												.get(java.util.Date.from(everyday.atStartOfDay(ZoneId.systemDefault())
														.toInstant())).sparsenessTrend3;
										isVald = allAssetPricesMapping.get(key).get(java.util.Date.from(
												everyday.atStartOfDay(ZoneId.systemDefault()).toInstant())).isVald;
										densenessWidth = allAssetPricesMapping.get(key).get(java.util.Date.from(everyday
												.atStartOfDay(ZoneId.systemDefault()).toInstant())).chipDensenessWidth;
										spotChipDensenessWidth = allAssetPricesMapping.get(key)
												.get(java.util.Date.from(everyday.atStartOfDay(ZoneId.systemDefault())
														.toInstant())).spotChipDensenessWidth;
										MA5Trend = allAssetPricesMapping.get(key).get(java.util.Date.from(
												everyday.atStartOfDay(ZoneId.systemDefault()).toInstant())).MA5Trend;
										MA5sTrendWithoutLastMA5 = allAssetPricesMapping.get(key)
												.get(java.util.Date.from(everyday.atStartOfDay(ZoneId.systemDefault())
														.toInstant())).MA5sTrendWithoutLastMA5;
										MA10Trend = allAssetPricesMapping.get(key).get(java.util.Date.from(
												everyday.atStartOfDay(ZoneId.systemDefault()).toInstant())).MA10Trend;
										MA30Trend = allAssetPricesMapping.get(key).get(java.util.Date.from(
												everyday.atStartOfDay(ZoneId.systemDefault()).toInstant())).MA30Trend;
										closePriceTrend = allAssetPricesMapping.get(key)
												.get(java.util.Date.from(everyday.atStartOfDay(ZoneId.systemDefault())
														.toInstant())).closePriceTrend;
										longBackwardsSparseness = allAssetPricesMapping.get(key)
												.get(java.util.Date.from(everyday.atStartOfDay(ZoneId.systemDefault())
														.toInstant())).sparsenessLong;
										superLongBackwardsSparseness = allAssetPricesMapping.get(key)
												.get(java.util.Date.from(everyday.atStartOfDay(ZoneId.systemDefault())
														.toInstant())).sparsenessSuperLong;
										midBackwardsSparseness = allAssetPricesMapping.get(key)
												.get(java.util.Date.from(everyday.atStartOfDay(ZoneId.systemDefault())
														.toInstant())).sparsenessMid;
										shortBackwardsSparseness = allAssetPricesMapping.get(key)
												.get(java.util.Date.from(everyday.atStartOfDay(ZoneId.systemDefault())
														.toInstant())).sparsenessShort;
										superShortBackwardsSparseness = allAssetPricesMapping.get(key)
												.get(java.util.Date.from(everyday.atStartOfDay(ZoneId.systemDefault())
														.toInstant())).sparsenessSuperShort;
										todayOpen = allAssetPricesMapping.get(key).get(java.util.Date.from(
												everyday.atStartOfDay(ZoneId.systemDefault()).toInstant())).openPrice;
										todayClose = allAssetPricesMapping.get(key).get(java.util.Date.from(
												everyday.atStartOfDay(ZoneId.systemDefault()).toInstant())).closePrice;
										todayHigh = allAssetPricesMapping.get(key).get(java.util.Date.from(
												everyday.atStartOfDay(ZoneId.systemDefault()).toInstant())).highPrice;
										todayLow = allAssetPricesMapping.get(key).get(java.util.Date.from(
												everyday.atStartOfDay(ZoneId.systemDefault()).toInstant())).lowPrice;
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
										MACDGoldCross = allAssetPricesMapping.get(key).get(java.util.Date.from(everyday
												.atStartOfDay(ZoneId.systemDefault()).toInstant())).MACDGoldCross;
										BollUpperTrack = allAssetPricesMapping.get(key).get(java.util.Date.from(
												everyday.atStartOfDay(ZoneId.systemDefault()).toInstant())).bollUp;
										BollMidTrack = allAssetPricesMapping.get(key).get(java.util.Date.from(
												everyday.atStartOfDay(ZoneId.systemDefault()).toInstant())).bollMid;
										BollLowerTrack = allAssetPricesMapping.get(key).get(java.util.Date.from(
												everyday.atStartOfDay(ZoneId.systemDefault()).toInstant())).bollDown;
										//BollMidTrend3 = allAssetPricesMapping.get(key).get(java.util.Date.from(everyday
										//		.atStartOfDay(ZoneId.systemDefault()).toInstant())).bollMidTrend3;
										// volatility = allAssetPricesMapping.get(key).get(java.util.Date.from(
										// everyday.atStartOfDay(ZoneId.systemDefault()).toInstant())).volatility;
										closePriceTrend60 = allAssetPricesMapping.get(key)
												.get(java.util.Date.from(everyday.atStartOfDay(ZoneId.systemDefault())
														.toInstant())).closePriceTrend60;
										Sparseness = new double[6];
										forwardSparse = false;
										forwardSparseness = 0;
										for (int i = 0; i < 4; i++) {
											if (todayIndex + i < simulationDateRange.size() && allAssetPricesMapping
													.get(key)
													.get(java.util.Date.from(simulationDateRange.get(todayIndex + i)
															.atStartOfDay(ZoneId.systemDefault())
															.toInstant())) == null) {
												break;
											} else if (todayIndex + i < simulationDateRange.size()) {
												Sparseness[i] = allAssetPricesMapping.get(key)
														.get(java.util.Date.from(simulationDateRange.get(todayIndex + i)
																.atStartOfDay(ZoneId.systemDefault())
																.toInstant())).sparsenessShort;

												if ((Sparseness[i]) > forwardSparsenessThreshold) {
													forwardSparseness = Sparseness[i];
													forwardSparse = true;
													break;
												}
											}
										}
										backwardMA5s = new ArrayList<Double>();
										float todayClosePrice = allAssetPricesMapping.get(key)
												.get(java.util.Date.from(simulationDateRange.get(todayIndex)
														.atStartOfDay(ZoneId.systemDefault()).toInstant())).closePrice;

										if (runSuperLong == true && densenessFound < densenessThreshold
												&& densenessFound > 0 && Math.abs(todayChange) <= dayChangeThreshold &&
												// todayChange > 0 &&
												superLongBackwardsSparseness <= superLongBackwardsSparsenessThreshold
										/*
										 * && UtilityAsset.queryVolatility(java.util.Date.from(simulationDateRange.get(
										 * todayIndex) .atStartOfDay(ZoneId.systemDefault()) .toInstant()), key, stmt) <
										 * volatilityThreshold && Math.abs(closePriceTrend60)<
										 * closePriceTrend60Threshold
										 */
										) { List<AssetStatus> assetStatusList = new LinkedList<AssetStatus>();
											max = min = 0;
											for (int i = 0; todayIndex + i < simulationDateRange.size(); i++) {
												if (allAssetPricesMapping.get(key)
														.get(java.util.Date.from(simulationDateRange.get(todayIndex + i)
																.atStartOfDay(ZoneId.systemDefault())
																.toInstant())) == null) {
													break;
												}
												assetStatusList.add(allAssetPricesMapping.get(key)
														.get(java.util.Date.from(simulationDateRange.get(todayIndex + i)
																.atStartOfDay(ZoneId.systemDefault()).toInstant())));
											}
											AssetStatus maxAS = Collections.max(assetStatusList,
													utilityTB.new AscendingOrder());
											AssetStatus minAS = Collections.min(assetStatusList,
													utilityTB.new AscendingOrder());

											try {
												// java.util.Date thruBreakerRunningDate = sortedDays
												// .get(sortedDays.size() - 1);
												/*
												 * double runningDateClose = allAssetPricesMapping.get(key)
												 * .get(thruBreakerRunningDate).closePrice; double runningDateChange =
												 * allAssetPricesMapping.get(key)
												 * .get(thruBreakerRunningDate).changeRate; //sd4 =
												 * allAssetPricesMapping.get(key).get(thruBreakerRunningDate).sd4;
												 * String sdExceedingWording = (runningDateClose / (1 +
												 * runningDateChange) runningDateChange >= 3 * sd4) ?
												 * ", with exceeding sd " + (runningDateClose / (1 + runningDateChange)
												 * runningDateChange) / (sd4) + " times,": ""; double sdExceeding =
												 * (runningDateClose / (1 + runningDateChange) runningDateChange) /
												 * (sd4);
												 */
												// String sdExceedingWording = "";
												// double sdExceeding = 0;
												sd4 = 0;
												String description = key + "(" + name
														+ ") got superLong sparseness DENSE("
														+ String.format("%.4f", superLongBackwardsSparseness)
														+ ") and closePriceTrend60("
														+ String.format("%.4f", closePriceTrend60)
														+ ") and todayChange(" + String.format("%.4f", todayChange)
														+ ") on " + everyday + " with max = " + maxAS.closePrice + "("
														+ String.format("%.2f",
																(maxAS.closePrice / todayClosePrice - 1) * 100)
														+ "%) on " + maxAS.tradeDate + " of "
														+ everyday.until(maxAS.tradeDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate(), ChronoUnit.DAYS)
														+ " days away, and min = " + minAS.closePrice + "("
														+ String.format("%.2f",
																(minAS.closePrice / todayClosePrice - 1) * 100)
														+ "%) on " + minAS.tradeDate + " of "
														+ everyday.until(minAS.tradeDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate(), ChronoUnit.DAYS)
														+ " days away, Diff = "
														+ String.format("%.2f", ((maxAS.closePrice - minAS.closePrice)
																/ todayClosePrice) * 100)
														+ "%";

												System.out.println(description);
												denseStockList.add(key + ": " + description);
												String recordDenseStock = "insert into denseness (stock_code, type, select_date, denseness, backwards_Sparseness, open, close, max, min, max_day, min_day, sd4, sd_exceeding, description, name) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
												preparedStmt = conn.prepareStatement(recordDenseStock);
												preparedStmt.setString(1, key);
												preparedStmt.setString(2, "super long");
												preparedStmt.setDate(3, java.sql.Date.valueOf(everyday));
												preparedStmt.setDouble(4, closePriceTrend60);
												preparedStmt.setDouble(5, superLongBackwardsSparseness);
												preparedStmt.setDouble(6, todayOpen);
												preparedStmt.setDouble(7, todayClose);
												preparedStmt.setDouble(8, Double.valueOf(String.format("%.2f",
														(maxAS.closePrice / todayClosePrice - 1) * 100)));
												preparedStmt.setDouble(9, Double.valueOf(String.format("%.2f",
														(minAS.closePrice / todayClosePrice - 1) * 100)));
												preparedStmt.setDate(10, java.sql.Date.valueOf(maxAS.tradeDate.toString()));
												preparedStmt.setDate(11, java.sql.Date.valueOf(minAS.tradeDate.toString()));
												preparedStmt.setDouble(12, sd4);
												preparedStmt.setDouble(13, 0);
												preparedStmt.setString(14, description);
												preparedStmt.setString(15, name);
												preparedStmt.execute();
											} catch (java.lang.Exception nullEx) {
												continue;
											}
										}

										if (runMidDense == true && densenessFound > 0
												&& midBackwardsSparseness <= midBackwardsSparsenessThreshold
										// && forwardSparse == true
										// && superShortBackwardsSparseness <= superShortBackwardsSparsenessThreshold
										) {
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
											java.util.Date thruBreakerRunningDate = sortedDays
													.get(sortedDays.size() - 1);
											try {
												double runningDateClose = allAssetPricesMapping.get(key)
														.get(thruBreakerRunningDate).closePrice;
												double runningDateChange = allAssetPricesMapping.get(key)
														.get(thruBreakerRunningDate).changeRate;
												/*
												 * sd4 = allAssetPricesMapping.get(key).get(thruBreakerRunningDate).sd4;
												 * String sdExceedingWording = (runningDateClose / (1 +
												 * runningDateChange) runningDateChange >= 3 * sd4) ?
												 * ", with exceeding sd " + (runningDateClose / (1 + runningDateChange)
												 * runningDateChange) / (sd4) + " times" : ""; double sdExceeding =
												 * (runningDateClose / (1 + runningDateChange) runningDateChange) /
												 * (sd4);
												 */
												sd4 = 0;
												String sdExceedingWording = "";
												double sdExceeding = 0;
												String description = key + "(" + name + ") got mid sparseness DENSE("
														+ String.format("%.4f", midBackwardsSparseness)
														+ ") and forwardSparseness("
														+ String.format("%.4f", forwardSparseness)
														+ ") and upwardTrend(" + String.format("%.4f", MA5Trend)
														+ ") on " + everyday + sdExceedingWording + ", with max = "
														+ max + "("
														+ String.format("%.2f", (max / todayClosePrice - 1) * 100)
														+ "%)" + " and min = " + min + "("
														+ String.format("%.2f", (min / todayClosePrice - 1) * 100)
														+ "%), Diff = "
														+ String.format("%.2f", ((max - min) / todayClosePrice) * 100)
														+ "%";

												System.out.println(description);
												denseStockList.add(key + ": " + description);
												String recordDenseStock = "insert into denseness (stock_code, type, select_date, denseness, backwards_Sparseness, open, close, max, min, sd4, sd_exceeding, description, name) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
												preparedStmt = conn.prepareStatement(recordDenseStock);
												preparedStmt.setString(1, key);
												preparedStmt.setString(2, "mid");
												preparedStmt.setDate(3, java.sql.Date.valueOf(everyday));
												preparedStmt.setDouble(4, densenessFound);
												preparedStmt.setDouble(5, midBackwardsSparseness);
												preparedStmt.setDouble(6, todayOpen);
												preparedStmt.setDouble(7, todayClose);
												preparedStmt.setDouble(8, Double.valueOf(
														String.format("%.2f", (max / todayClosePrice - 1) * 100)));
												preparedStmt.setDouble(9, Double.valueOf(
														String.format("%.2f", (min / todayClosePrice - 1) * 100)));
												preparedStmt.setDouble(10, sd4);
												preparedStmt.setDouble(11, sdExceeding);
												preparedStmt.setString(12, description);
												preparedStmt.setString(13, name);
												preparedStmt.execute();
											} catch (java.lang.Exception nullEx) {
												continue;
											}
										}

										if (runDownwardTrend == true && todayClose < todayMA5price
												&& todayClose < todayMA10price && todayClose < todayMA20price
												&& todayClose < todayMA30price
												&& (densenessWidth > densenessWidthThreshold
														&& sparsenessTrend >= densenessTrendThreshold
												// && todayClose <= BollLowerTrack
												)
										// && MACDGoldCross.equals("金叉")
										) {
											List<AssetStatus> assetStatusList = new LinkedList<AssetStatus>();
											max = min = 0;
											for (int i = 0; todayIndex + i < simulationDateRange.size(); i++) {
												if (allAssetPricesMapping.get(key)
														.get(java.util.Date.from(simulationDateRange.get(todayIndex + i)
																.atStartOfDay(ZoneId.systemDefault())
																.toInstant())) == null) {
													break;
												}
												assetStatusList.add(allAssetPricesMapping.get(key)
														.get(java.util.Date.from(simulationDateRange.get(todayIndex + i)
																.atStartOfDay(ZoneId.systemDefault()).toInstant())));
											}
											// Collections.sort(assetStatusList, new DescendingOrder() );
											AssetStatus maxAS = Collections.max(assetStatusList,
													utilityTB.new AscendingOrder());
											AssetStatus minAS = Collections.min(assetStatusList,
													utilityTB.new AscendingOrder());
											String description = key + "(" + name
													+ ") got DOWNWARD with densenessTrend("
													+ String.format("%.4f", sparsenessTrend) + ") and densenessWidth("
													+ String.format("%.4f", densenessWidth) + ") on " + everyday
													+ " with max = " + maxAS.closePrice + "("
													+ String.format("%.2f",
															(maxAS.closePrice / todayClosePrice - 1) * 100)
													+ "%) on " + maxAS.tradeDate + " of "
													+ (priceDateRange.indexOf(java.sql.Date.valueOf(maxAS.tradeDate.toString()).toLocalDate())
															- priceDateRange.indexOf(everyday))
													+ " days away, and min = " + minAS.closePrice + "("
													+ String.format("%.2f",
															(minAS.closePrice / todayClosePrice - 1) * 100)
													+ "%) on " + minAS.tradeDate + " of "
													+ (priceDateRange.indexOf(java.sql.Date.valueOf(minAS.tradeDate.toString()).toLocalDate())
															- priceDateRange.indexOf(everyday))
													+ " days away, Diff = "
													+ String.format("%.2f",
															((maxAS.closePrice - minAS.closePrice) / todayClosePrice)
																	* 100)
													+ "%";
											System.out.println(description);
											downwardStockList.add(key + ": " + description);
											String recordDenseStock = "insert into downward(stock_code, select_date, open, close, change_rate, max, min, max_away, min_away, max_day, min_day, denseness_trend, description, name) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
											preparedStmt = conn.prepareStatement(recordDenseStock);
											preparedStmt.setString(1, key);
											preparedStmt.setDate(2, java.sql.Date.valueOf(everyday));
											preparedStmt.setDouble(3, todayOpen);
											preparedStmt.setDouble(4, todayClose);
											preparedStmt.setDouble(5, todayChange);
											preparedStmt.setDouble(6, Double.valueOf(String.format("%.2f",
													(maxAS.closePrice / todayClosePrice - 1) * 100)));
											preparedStmt.setDouble(7, Double.valueOf(String.format("%.2f",
													(minAS.closePrice / todayClosePrice - 1) * 100)));
											preparedStmt.setInt(8, priceDateRange.indexOf(maxAS.tradeDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate())
													- priceDateRange.indexOf(everyday));
											preparedStmt.setInt(9, priceDateRange.indexOf(minAS.tradeDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate())
													- priceDateRange.indexOf(everyday));
											preparedStmt.setDate(10, java.sql.Date.valueOf(maxAS.tradeDate.toString()));
											preparedStmt.setDate(11, java.sql.Date.valueOf(minAS.tradeDate.toString()));
											preparedStmt.setDouble(12, sparsenessTrend);
											preparedStmt.setString(13, description);
											preparedStmt.setString(14, name);
											if (todayClosePrice != 0) {
												preparedStmt.execute();
											}
											conn.commit();
										}

										if (runSpotChip == true
												&& spotChipDensenessWidth <= spotChipDensenessWidthThreshold
												&& sparsenessTrend < densenessTrendThreshold) {
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
											String description = key + "(" + name
													+ ") got spot chip dense with spotChipDensenessWidth("
													+ String.format("%.4f", spotChipDensenessWidth)
													+ ") and sparsenessTrend(" + String.format("%.4f", sparsenessTrend)
													+ ") on " + everyday + " with max = " + max + "("
													+ String.format("%.2f", (max / todayClosePrice - 1) * 100) + "%)"
													+ " and min = " + min + "("
													+ String.format("%.2f", (min / todayClosePrice - 1) * 100) + "%), "
													+ "Diff = "
													+ String.format("%.2f", ((max - min) / todayClosePrice) * 100)
													+ "%";
											System.out.println(description);
											denseStockList.add(key + ": " + description);

											String recordDenseStock = "insert into denseness (stock_code, type, select_date, spot_denseness_width, sparseness_trend, open, close, max, min, description, name) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
											preparedStmt = conn.prepareStatement(recordDenseStock);
											preparedStmt.setString(1, key);
											preparedStmt.setString(2, "spot chip");
											preparedStmt.setDate(3, java.sql.Date.valueOf(everyday));
											preparedStmt.setDouble(4, spotChipDensenessWidth);
											preparedStmt.setDouble(5, sparsenessTrend);
											preparedStmt.setDouble(6, todayOpen);
											preparedStmt.setDouble(7, todayClose);
											preparedStmt.setDouble(8, Double
													.valueOf(String.format("%.2f", (max / todayClosePrice - 1) * 100)));
											preparedStmt.setDouble(9, Double
													.valueOf(String.format("%.2f", (min / todayClosePrice - 1) * 100)));
											preparedStmt.setString(10, description);
											preparedStmt.setString(11, name);
											preparedStmt.execute();
											conn.commit();
										}
										if (runSparse == true && isVald == true && MA30Trend > MA30TrendThreshold
										// && closePriceTrend > 0
										// && MA5sTrendWithoutLastMA5 > 0
										) {
											allAssetPricesMapping.get(key).get(java.util.Date.from(everyday
													.atStartOfDay(ZoneId.systemDefault()).toInstant())).isSparse = true;
											System.out.println(key + "(" + name + ") got MA30Trend " + MA30Trend + "on "
													+ everyday);
											List<AssetStatus> assetStatusList = new LinkedList<AssetStatus>();
											max = min = 0;
											for (int i = 0; todayIndex + i < simulationDateRange.size(); i++) {
												if (allAssetPricesMapping.get(key)
														.get(java.util.Date.from(simulationDateRange.get(todayIndex + i)
																.atStartOfDay(ZoneId.systemDefault())
																.toInstant())) == null) {
													break;
												}
												assetStatusList.add(allAssetPricesMapping.get(key)
														.get(java.util.Date.from(simulationDateRange.get(todayIndex + i)
																.atStartOfDay(ZoneId.systemDefault()).toInstant())));
											}
											AssetStatus maxAS = Collections.max(assetStatusList,
													utilityTB.new AscendingOrder());
											AssetStatus minAS = Collections.min(assetStatusList,
													utilityTB.new AscendingOrder());

											closePrices = new LinkedList<Float>();
											String description = key + "(" + name
													+ ") got large Sparseness with sparsenessTrend("
													+ String.format("%.4f", sparsenessTrend) + ") and closePriceTrend("
													+ String.format("%.4f", closePriceTrend) + ") on " + everyday
													+ "(with close of " + todayClose + ") with max = "
													+ maxAS.closePrice + "("
													+ String.format("%.2f",
															(maxAS.closePrice / todayClosePrice - 1) * 100)
													+ "%) on " + maxAS.tradeDate + " and min = " + minAS.closePrice
													+ "("
													+ String.format("%.2f",
															(minAS.closePrice / todayClosePrice - 1) * 100)
													+ "%), on " + minAS.tradeDate + ", Diff = "
													+ String.format("%.2f",
															((maxAS.closePrice - minAS.closePrice) / todayClosePrice)
																	* 100)
													+ "%";
											System.out.println(description);
											denseStockList.add(key + ": " + description);
											String recordDenseStock = "insert into denseness (stock_code, type, select_date, sparseness_trend, open, close, max, min, max_day, min_day, description, name) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
											preparedStmt = conn.prepareStatement(recordDenseStock);
											preparedStmt.setString(1, key);
											preparedStmt.setString(2, "upward sparse");
											preparedStmt.setDate(3, java.sql.Date.valueOf(everyday));
											preparedStmt.setDouble(4, 0);
											preparedStmt.setDouble(5, todayOpen);
											preparedStmt.setDouble(6, todayClose);
											preparedStmt.setDouble(7, Double.valueOf(String.format("%.2f",
													(maxAS.closePrice / todayClosePrice - 1) * 100)));
											preparedStmt.setDouble(8, Double.valueOf(String.format("%.2f",
													(minAS.closePrice / todayClosePrice - 1) * 100)));
											preparedStmt.setDate(9, java.sql.Date.valueOf(maxAS.tradeDate.toString()));
											preparedStmt.setDate(10, java.sql.Date.valueOf(minAS.tradeDate.toString()));
											preparedStmt.setString(11, description);
											preparedStmt.setString(12, name);
											if (todayClosePrice != 0) {
												preparedStmt.execute();
											}
											conn.commit();
											java.util.Date date = java.util.Date
													.from(everyday.atStartOfDay(ZoneId.systemDefault()).toInstant());
											allAssetPricesMapping.get(key).get(date).isSparse = true;
											if (allAssetPricesMapping.get(key).get(date).isSparse == true) {
												allAssetPricesMapping.get(key)
														.get(date).sparseOriginatedDate = traceSparseOriginatedDate(
																allAssetPricesMapping.get(key).get(date));
												System.out.println(key + "@" + date + " sparse Originated Date: "
														+ allAssetPricesMapping.get(key)
																.get(date).sparseOriginatedDate);
											} else {
												allAssetPricesMapping.get(key).get(date).sparseOriginatedDate = null;
											}
										}

										boolean runBoll = false;

										if (runBoll) {
											if (BollMidTrend3 > 0.0 && todayClose - BollMidTrack < 0
											// < (BollMidTrack - BollLowerTrack)/2/BollMidTrack* 0.1
													&& (BollMidTrack - BollLowerTrack) / 2 / BollMidTrack > 0.05
													&& todayClose > todayOpen) {
												java.util.Date date = java.util.Date.from(
														everyday.atStartOfDay(ZoneId.systemDefault()).toInstant());
												System.out.println("Boll dense: Date: " + key + "@" + date);
												if (bollMap.get(date) != null) {
													// if(!bollMap.get(accessPriceDateRange(date, -1)).contains(key)) {
													bollMap.get(date).add(key);
													// }
												} else {
													bollList = new LinkedList<String>();
													bollList.add(key);
													bollMap.put(date, bollList);
												}
											}
										}
									}
								}
								denseCandidates.put(everyday, denseStockList);
								downwardCandidates.put(everyday, downwardStockList);
							}
						}

						List<java.util.Date> bollDates = new LinkedList<java.util.Date>();
						bollDates.addAll(bollMap.keySet());
						bollDates.sort(null);
						for (java.util.Date bollDate : bollDates) {
							System.out.println(
									"\n" + bollDate.toString().split(" ")[5] + " - " + bollDate.toString().split(" ")[1]
											+ " - " + bollDate.toString().split(" ")[2] + ": ");
							for (String s : bollMap.get(bollDate)) {
								if (bollMap.get(accessPriceDateRange(bollDate, -1)) != null
										&& !bollMap.get(accessPriceDateRange(bollDate, -1)).contains(s)) {
									System.out.print(", " + s + ", ");
								}
							}
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
									denseNdownwardList.put(stockCode.split(":")[0].substring(2), "");
								}
							}
						}
						for (LocalDate d : downwardCandidates.keySet()) {
							if (d.isAfter(LocalDate.of(2024, 10, 1))) {
								for (String stockCode : downwardCandidates.get(d)) {
									denseNdownwardList.put(stockCode.split(":")[0].substring(2), "");
								}
							}
						}
						for (String s : denseNdownwardList.keySet()) {
							System.out.println(s);
						}
						System.out.println("quant trading end.");

						conn.commit();

						// FileOutputStream fos = new FileOutputStream("xyz.txt");
						// ObjectOutputStream oos = new ObjectOutputStream(fos);
						// oos.writeObject(a);
						Administrator.updateAssetInfo(updateUpwardSparseSelectDateSince);
					}

					System.out.println("quant trading MA upImpaler Gains since: " + simulationStart + ", "
							+ quadraticSparseness + ", " + quantTraingGains);
					// System.out.println("simulationDateRange: " + simulationDateRange);
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