package StockMarket;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import StockMarket.Portfolio.AssetBuyingPlan;
import StockMarket.Portfolio.TradingPoint;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

// Java program to send email 

import java.util.*; 
import javax.mail.*; 
import javax.mail.internet.*; 
import javax.activation.*; 
import javax.mail.Session; 
import javax.mail.Transport; 


import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;


import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


class DescendingPriceOrder implements Comparator<ActualPriceDateSet> {
	public int compare(ActualPriceDateSet o1, ActualPriceDateSet o2) {
		Float i1 = (Float) o1.getActualPrice();
		Float i2 = (Float) o2.getActualPrice();
		return i1.compareTo(i2);
	}
}

class DescendingRankingOrder implements Comparator<RankedStock> {
	public int compare(RankedStock o1, RankedStock o2) {
		Integer i1 = (Integer) o1.getRanking_low();
		Integer i2 = (Integer) o2.getRanking_low();
		return -i1.compareTo(i2);
	}
}


class RankedStock{
	public RankedStock(String stockCode, int ranking_low) {
		super();
		this.stockCode = stockCode;
		this.ranking_low = ranking_low;
	}
		
	public String getStockCode() {
		return stockCode;
	}
	public void setStockCode(String stockCode) {
		this.stockCode = stockCode;
	}

	public int getRanking_low() {
		return ranking_low;
	}
	public void setRanking_low(int ranking_low) {
		this.ranking_low = ranking_low;
	}
	public int getRanking_high() {
		return ranking_high;
	}
	public void setRanking_high(int ranking_high) {
		this.ranking_high = ranking_high;
	}
	String stockCode;
	int ranking_low;
	int ranking_high;
	
}

public class Rebound{
	
	static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://localhost:20/mysql";
	// Database credentials
	static final String USER = "root";
	static final String PASS = "";
	static Connection conn = null;
	static Statement stmt = null;
	public Rebound(){
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
		// Handle errors for JDBC
		se.printStackTrace();
	} catch (Exception e) {
		// Handle errors for Class.forName
		e.printStackTrace();
	} finally {
	}
	}

	public static void main(String args[]) throws InterruptedException {
		  LocalDate start = LocalDate.of(2024, 02, 29); 
		  LocalDate end = LocalDate.of(2024, 04, 26); 
		  LocalDate initialDate = LocalDate.of(2024, 04, 01); 
		  java.sql.Date startDate= java.sql.Date.valueOf(start);
		  new Rebound();
		  //Portfolio batchBuyingStrategyPortfolio = new Portfolio(1000000.0, startDate, "ALL", 10000000000.0, conn, stmt);
		    long dateLong;
		    LocalDate ld = LocalDate.of(2024, 6, 21);
	        String YINN = "select * from YINN where date='"+ld+"'";
		    String update_YINN = "update YINN set date=? where date_long=?";       
		    String ZYX_stock="select distinct 股票代码 from daily_snapshot where is_index='N' or is_index=null; ";   
            
		    int month,day,year, rankingNo;
		    String stockCode, investigatingDateStr, apdsStr, adpsMonth, apdsDay;
		    float close;
		    ResultSet rs;
		    Date lowDay;
		    //System.out.println("YINN: "+YINN);
            Map<String, List<ActualPriceDateSet>> nadirs = new HashMap<String, List<ActualPriceDateSet>>();
            ActualPriceDateSet nadir;
		    System.out.println("ZYX_stock: "+ZYX_stock);
		    investigatingDateStr="2024-08-07";
		    Date investigationDate= Date.valueOf(investigatingDateStr);
		    List<ActualPriceDateSet> forAStock;
		    String ZYX_price_history;
		    if (args[0].equals("future")) {
		    	ZYX_price_history="select * from daily_snapshot order by 前复权价;";
		    }
		    else if(args[0].equals("cutoff")) {
		    	ZYX_price_history="select * from daily_snapshot where 交易日期 <= '"+investigatingDateStr+"' order by 前复权价;";
		    }
		    else {
		    	ZYX_price_history="select * from daily_snapshot order by 前复权价;";
		    }
		    try {
				rs = stmt.executeQuery(ZYX_price_history);
				/*while (rs.next()) {
					dateLong = rs.getLong("date_long");
			        LocalDateTime dateTime = LocalDateTime.ofEpochSecond(dateLong, 0, ZoneOffset.UTC);
			        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("EEEE,MM,d,yyyy, h:mm,a", Locale.ENGLISH);
			        String formattedDate = dateTime.format(formatter);
			        System.out.println(formattedDate); // Tuesday,11,1,2011 12:00,AM
			        month = Integer.parseInt(formattedDate.split(",")[1]);    
			        day = Integer.parseInt(formattedDate.split(",")[2]);  
			        year = Integer.parseInt(formattedDate.split(",")[3]);    
			        System.out.println("year: "+year);
			        
					java.sql.Date date= java.sql.Date.valueOf(formattedDate.split(",")[3]+"-"+formattedDate.split(",")[1]+"-"+formattedDate.split(",")[2]);
			        PreparedStatement preparedStmt = conn.prepareStatement(update_YINN);
			        preparedStmt.setDate(1, date);
			        preparedStmt.setLong(2, dateLong);
			        preparedStmt.execute();
				}
				*/

				rankingNo=0;
				while (rs.next()) {
					//System.out.println(rs.getLong("date_long"));
					stockCode=rs.getString("股票代码");
					System.out.println(rs.getString("股票代码"));
						close=(float)rs.getFloat("前复权价");
						lowDay=rs.getDate("交易日期");
						nadir=new ActualPriceDateSet(lowDay.toLocalDate(), close);
						if (nadirs.get(stockCode)!=null) {
							forAStock=(List<ActualPriceDateSet>) nadirs.get(stockCode);
							}
						else {
							forAStock = new LinkedList<ActualPriceDateSet>();
						}
						forAStock.add(nadir);
						nadirs.put(stockCode, forAStock);
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		    
		    Set<String> stocks = nadirs.keySet();
		    List<ActualPriceDateSet> priceLine;
		    
		    DescendingPriceOrder dpo= new DescendingPriceOrder();
		    StringBuilder sb;
		    List<RankedStock> lowestPricedStocks= new ArrayList<RankedStock>();
		    String hibernate_nadir_sql;
		    PreparedStatement preparedStmt;
		    for (String s: stocks) {
		    	priceLine=nadirs.get(s);
		    	priceLine.sort(dpo);
		    	rankingNo=0;
		    	for (ActualPriceDateSet apds: priceLine) {
		    		rankingNo++;
		    		sb = new StringBuilder();
		    		if ((apds.getActualPriceDate().getMonthValue()+"").length()<2) {
		    			sb.append('0');
		    		}
		    		sb.append(apds.getActualPriceDate().getMonthValue());
		    		adpsMonth = sb.toString();
		    		sb = new StringBuilder();
		    		if ((apds.getActualPriceDate().getDayOfMonth()+"").length()<2) {
		    			sb.append('0');
		    		}
		    		sb.append(apds.getActualPriceDate().getDayOfMonth());
		    		apdsDay = sb.toString();
		    		apdsStr=apds.getActualPriceDate().getYear()+"-"+adpsMonth+"-"+apdsDay;
		    	    //System.out.println(s+" apdsStr: "+apdsStr);
		    	    if (apdsStr.equals(investigatingDateStr)) {
		    	    	if (rankingNo<= Integer.parseInt(args[1]) ) {
		    	    	System.out.println(s+", at "+apdsStr+" ranked "+rankingNo+"th lowest");
		    	    	//System.out.println(s+" break at "+apdsStr);
		    	    	hibernate_nadir_sql = " insert into rebound (code, investigation_date, type, ranking_no) values (?, ?, ?, ?) ";
		    	    	try {
							preparedStmt = conn.prepareStatement(hibernate_nadir_sql);
							preparedStmt.setString(1, s);
							preparedStmt.setDate(2, Date.valueOf(apdsStr));
							preparedStmt.setString(3, args[0]);
							preparedStmt.setInt(4, rankingNo);
							preparedStmt.execute();
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
		    	    	
		    	    	lowestPricedStocks.add(new RankedStock(s, rankingNo));
		    	    	}
		    	    	break;
		    	    }
		    	}
		    }
		    lowestPricedStocks.sort(new DescendingRankingOrder().reversed());
		    
		    String lastTradingDayClose="select * from daily_snapshot where 交易日期 = (select max(交易日期) from daily_snapshot) "
		    		+ " and 股票代码 in (";
		    
		    for (RankedStock rankedStock: lowestPricedStocks) {
		    	System.out.println("\""+rankedStock.getStockCode()+"\",");
		    	lastTradingDayClose=lastTradingDayClose+"'"+rankedStock.getStockCode()+"',";
		    }
		    lastTradingDayClose=lastTradingDayClose+"'');";
		    if (lowestPricedStocks.isEmpty()) {
		    	System.out.println("lowest Priced Stocks empty.");
		    }
		    try {
				rs = stmt.executeQuery(lastTradingDayClose);
			
				while (rs.next()) {
					stockCode=rs.getString("股票代码");
					close=(float)rs.getFloat("前复权价");
					System.out.println("\""+stockCode+"|"+close+"\",");
				}
		    } catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
}

		//String dummy="sz002174 | 游族网络| Not yet back! | signifying on 2024-02-05, with 3-D change of 10.79%, 6-D change of 14.89% ｜ 2024-02-05 | lowestClose: 8.53, openAfterLowest: 8.1, reboundOpen: 9.37, difference(lowestClose-reboundOpen)/reboundOpen: -8.96%";
		//System.out.println(dummy.split("\\|")[3]);
		//System.out.println(dummy.split("\\|")[4]);
		//Date signifyingDay = new SimpleDateFormat("yyyy-MM-dd").parse(dummy.split("\\|")[4]);
		//DecimalFormat expf = new DecimalFormat("###,###,###,###");
		//System.out.println(expf.format(998569464));
		//System.out.println(signifyingDay);
		/*

			// email ID of Recipient. 
			String recipient = "zhoupengs@hotmail.com"; 

			// email ID of Sender. 
			String sender = "zhoupengs@hotmail.com"; 

			// using host as localhost 
			String host = "127.0.0.1"; 

			// Getting system properties 
			Properties properties = System.getProperties(); 

			// Setting up mail server 
			properties.setProperty("mail.smtp.host", host); 

			// creating session object to get properties 
			Session session = Session.getDefaultInstance(properties); 

			try
			{ 
				// MimeMessage object. 
				MimeMessage message = new MimeMessage(session); 

				// Set From Field: adding senders email to from field. 
				message.setFrom(new InternetAddress(sender)); 

				// Set To Field: adding recipient's email to from field. 
				message.addRecipient(Message.RecipientType.TO, new InternetAddress(recipient)); 

				// Set Subject: subject of the email 
				message.setSubject("This is Subject"); 

				// set body of the email. 
				message.setText("This is a test mail"); 

				// Send email. 
				Transport.send(message); 
				System.out.println("Mail successfully sent"); 
			} 
			catch (Exception mex) 
			{ 
				mex.printStackTrace(); 
			} 
		} 
		
*/
		
		/*
		Map<Date, List<Portfolio.TradingPoint>> gainMap=new HashMap<Date, List<Portfolio.TradingPoint>>();
		
		Portfolio portfolio= new Portfolio(new java.sql.Date(0), conn, stmt);
		List<Date> range=new LinkedList<Date>();
		double gainRateHighAVG = 0,gainRateCloseAVG = 0,gainRateOpenAVG=0,gainRateHighLowAverageAVG = 0;
		 String gain_stat="select distinct first_rising_date from first_rising where first_rising_date<= '2024-04-22' order by first_rising_date";
			try {
				ResultSet rs = stmt.executeQuery(gain_stat);
				while (rs.next()) {
					range.add(rs.getDate("first_rising_date"));
				}
				
				for (Date risingDay: range) {
					portfolio.firstRisingGainLossCal(risingDay, null, null, conn, stmt, gainMap);
				}			
				System.out.println("gainMap.keySet(): "+gainMap.keySet());
				Iterator<Date> i=gainMap.keySet().iterator();
				while (i.hasNext()) {
					Date firstRisingDay=i.next();
					for (TradingPoint sp: gainMap.get(firstRisingDay)) {
						gainRateCloseAVG+=sp.gainRateClose;
						gainRateOpenAVG+=sp.gainRateOpen;
						gainRateHighAVG+=sp.gainRateHigh;
						gainRateHighLowAverageAVG+=sp.gainRateHighLowAverage;
					}
					gainRateOpenAVG=gainRateOpenAVG/gainMap.get(firstRisingDay).size();
					gainRateCloseAVG=gainRateCloseAVG/gainMap.get(firstRisingDay).size();
					gainRateHighAVG=gainRateHighAVG/gainMap.get(firstRisingDay).size();
					gainRateHighLowAverageAVG=gainRateHighLowAverageAVG/gainMap.get(firstRisingDay).size();
					System.out.println("rising Day:"+firstRisingDay+ ": AVG(Open): "+ gainRateOpenAVG);
					System.out.println("rising Day:"+firstRisingDay+ ": AVG(Close): "+ gainRateCloseAVG);
					System.out.println("rising Day:"+firstRisingDay+ ": AVG(High): "+ gainRateHighAVG);
					System.out.println("rising Day:"+firstRisingDay+ ": AVG(HighLowAverage): "+ gainRateHighLowAverageAVG);
				}
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}*/

