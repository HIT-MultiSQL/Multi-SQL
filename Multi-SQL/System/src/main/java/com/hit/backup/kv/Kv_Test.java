package com.hit.backup.kv;

import java.util.ArrayList;

import javax.swing.plaf.basic.BasicInternalFrameTitlePane.RestoreAction;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Kv_Test {
	
	public String dbname = "test";
	public String host = "127.0.0.1";
	public String port = "5432";
	public String user = "postgres";   
	public String password = "postgres";

//	public Kv_Test(String dbname, String host, String port, String user, String password) {
//		this.dbname = dbname;
//		this.host = host;
//	    this.port = port;
//	    this.user = user;
//	    this.password = password;
//	}
	
	public  void load_data() throws SQLException, IOException, ClassNotFoundException {
		
	    Connection c = null;
	    Statement stmt = null;
	    String url = "jdbc:postgresql://127.0.0.1:" + port + "/"+dbname;
	    Class.forName("org.postgresql.Driver");
	    c= DriverManager.getConnection(url, user, password);
	    stmt = c.createStatement();	    	
		FileReader frworkload = new FileReader("/home/gaomeng/k-modal/data");
		BufferedReader bfworkload = new BufferedReader(frworkload);
		String str;
		while ((str = bfworkload.readLine()) != null) {
			String [] arr = str.split(" ");
			String my_sql = "INSERT INTO userlike (uid,peoples) VALUES ('"+arr[0]+"','" + arr[1] + "')";
			stmt.executeUpdate(my_sql);
		}
		System.out.println("success load  data to database!");
		bfworkload.close();
		frworkload.close();	
		stmt.close();
		c.commit();
		c.close();
		
	}
	
	public long  optimization_test(Integer op) throws SQLException, IOException {
	    Connection c = null;
	    Statement stmt = null;
    	String url = "jdbc:postgresql://127.0.0.1:" + port + "/"+dbname;
    	c= DriverManager.getConnection(url, user, password);
    	stmt = c.createStatement();
    	
    	stmt.executeUpdate("DROP INDEX IF EXISTS idx;");
    	stmt.executeUpdate("CREATE INDEX idx ON userlike USING btree (uid) WITH (fillfactor = 100);");
    	
		FileReader frworkload = new FileReader("/home/gaomeng/k-modal/workload");
		BufferedReader bfworkload = new BufferedReader(frworkload);
		
		long startTime = System.currentTimeMillis();
		String str;
		while ((str = bfworkload.readLine()) != null) {
			ResultSet rs = stmt.executeQuery(str);
		}
		long endTime = System.currentTimeMillis();
		
		long intelltime = endTime - startTime;
		bfworkload.close();
		frworkload.close();
		
    	stmt.executeUpdate("DROP INDEX IF EXISTS idx;");
    	stmt.executeUpdate("CREATE INDEX idx ON userlike USING hash (uid) WITH (fillfactor = 100);");
		FileReader frworkload1 = new FileReader("/home/gaomeng/k-modal/workload");
		BufferedReader bfworkload1 = new BufferedReader(frworkload1);
		long startTime1 = System.currentTimeMillis();
		String str1;
		while ((str1 = bfworkload1.readLine()) != null) {
			ResultSet rs = stmt.executeQuery(str1);
		}
		long endTime1 = System.currentTimeMillis();
		long peopletime = endTime1 - startTime1;
		bfworkload1.close();
		frworkload1.close();
		
    	stmt.executeUpdate("DROP INDEX IF EXISTS idx;");
    	stmt.executeUpdate("CREATE INDEX idx ON userlike USING hash (uid) WITH (fillfactor = 55);");
		FileReader frworkload2 = new FileReader("/home/gaomeng/k-modal/workload");
		BufferedReader bfworkload2 = new BufferedReader(frworkload2);
		long startTime2 = System.currentTimeMillis();
		String str2;
		while ((str2 = bfworkload2.readLine()) != null) {
			ResultSet rs = stmt.executeQuery(str2);
		}
		long endTime2 = System.currentTimeMillis();
		long defaulttime = endTime2 - startTime2;
		bfworkload2.close();
		frworkload2.close();
		
		if(op == 0) {
			return intelltime;
		}else if (op == 1) {
			return peopletime;
		}else {
			return defaulttime;
		}
			
	}
	
	public  ArrayList<String> multi_modal(String sql) throws SQLException, ClassNotFoundException{
		String quStr=sql.substring(sql.indexOf("(")+1,sql.indexOf(")"));
		String newstring =  "Select * from userlike where userlike.uid in ("+ quStr +");";
		Connection c = null;
	    Statement stmt = null;
	    Class.forName("org.postgresql.Driver");
	    String url = "jdbc:postgresql://127.0.0.1:" + port + "/"+dbname;
    	c= DriverManager.getConnection(url, user, password);
    	stmt = c.createStatement();
		ResultSet rs = stmt.executeQuery(newstring);
		ArrayList<String> relist =new ArrayList<String>();
		while(rs.next()){
			String res = rs.getString("uid") + " " + rs.getString("people");
			relist.add(res);
		}
		stmt.close();
		c.close();
		return relist;
	}
	
	
	public  ArrayList<String> kv_modal(String sql) throws SQLException, ClassNotFoundException{
	    Connection c = null;
	    Statement stmt = null;
	    Class.forName("org.postgresql.Driver");
	    String url = "jdbc:postgresql://127.0.0.1:" + port + "/"+dbname;
	    Class.forName("org.postgresql.Driver");
    	c= DriverManager.getConnection(url, user, password);
    	stmt = c.createStatement();
		ResultSet rs = stmt.executeQuery(sql);	
		ArrayList<String> relist =new ArrayList<String>();
		while(rs.next()){
			String res = rs.getString("uid") + " " + rs.getString("peoples");
			relist.add(res);
		}
		stmt.close();
		c.close();
		return relist;
	}
	
	
}
