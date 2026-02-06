package StockMarket;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;

public class TestClass1 {
	//Asset utilityAsset= new Asset();
	//http://api.mairui.club/hsrl/fscj/300002/cd5268626606b8b4ef
	/*  String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
	  String DB_URL = "jdbc:mariadb://localhost:20/mysql";
	  String USER = "root";
	  String PASS = "";
	  Connection conn;
	  Statement stmt;{
	try {
		Class.forName(JDBC_DRIVER);
		 conn = DriverManager.getConnection(
				DB_URL + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai", USER, PASS);
		stmt=conn.createStatement();
	} catch (Exception excp) {
		excp.printStackTrace();
	}
	  }
	*/
	
	public static void main() {
	URL url;
	try {
		url = new URL("http://api.mairui.club/hsrl/fscj/300002/cd5268626606b8b4ef");
		
		String jsonStr = IOUtils.toString(url, "UTF-8");
		
		System.out.println(jsonStr);
		
	} catch (MalformedURLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	}
	
	
	
}
