package com.hit.backup.kv;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.cassandra.thrift.Cassandra.login_args;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * Hello world!
 *
 */
public class kv_modal {

	public String scan_data_path;
	public String scan_workload_path;
	public String dbname = "test";
	public String host = "127.0.0.1";
	public String port = "5432";
	public String user = "postgres";
	public String password = "postgres";

	public ArrayList<String> scanData(String fileName) {
		this.scan_data_path = fileName;
		ArrayList<String> relist = new ArrayList<String>();
		String Details = "Path：/home/gaomeng/smart-index/data"+"\n"+
				"Type: dictionary"+"\n"+
				"Count: 8648"+"\n"+
				"Size: 269KB"+"\n"+
				"Example: {\"07485\": [\"02502\", \"01948\", \"00794\"]}";
		String configurations = "Name: userlik"+"\n"+
				"Persistence: storage_kv_a"+"\n"+
				"Database: postgresql"+"\n"+
				"IP: 219.217.229.74"+"\n"+
				"Port: 5432"+"\n"+
				"Db_name: test"+"\n+" +
				"collection_name: kv_a";
		String storage = "/home/gaomeng/smart-index/data";
		relist.add(Details);
		relist.add(configurations);
		relist.add(storage);
		return relist;
	}

	public Boolean uploadData() {
		return true;
	}

	public ArrayList<String> scanWorkload(String fileName) {
		this.scan_workload_path = fileName;
		ArrayList<String> relist = new ArrayList<String>();
		String Details = "Path：/home/gaomeng/smart-index/workloads/workload"+"\n"+
				"Read operation: 1,000"+"\n"+
				"write operation: 0"+"\n"+
				"type: read_only"+"\n"+
				"size: 45KB"+"\n"+
				"Example: { SELECT * from userlike where uid = '1235'}";
		String configurations = "Name: workkv_a"+"\n"+
				"IP: 219.217.229.74"+"\n"+
				"port: 5432"+"\n"+
				"position: \\server\\multi-database\\workload\\kv\\"+"\n"+
				"file_name: workkv_a";
		String storage = "/home/gaomeng/smart-index/workloads/workload";
		relist.add(Details);
		relist.add(configurations);
		relist.add(storage);
		return relist;
	}

	/**
	 * load workload into the workload folder
	 * 
	 * @param
	 */
	public boolean uploadWorkLoad() {
		return true;
	}

	/**
	 * 
	 * Recommend index based on loaded model
	 */
	public ArrayList<String> recommend() {
		Process proc;
		try {
			proc = Runtime.getRuntime().exec("python home/gaomeng/smart-index/test.py");
			proc.waitFor();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		ArrayList<String> relist = new ArrayList<String>();
		String s = readJsonFile("/home/gaomeng/smart-index/results");
		JSONObject jobj = JSON.parseObject(s);
		relist.add(jugeindex(jobj.getInteger("intell_index")));
		relist.add(jobj.getInteger("intell_fillfactor").toString());
		relist.add(jugeindex(jobj.getInteger("man_index")));
		relist.add(jobj.getInteger("man_fillfactor").toString());
		relist.add(jugeindex(jobj.getInteger("default_index")));
		relist.add(jobj.getInteger("default_fillfactor").toString());
		relist.add(jobj.getInteger("effencicy").toString());
		return relist;
	}
    // 推荐索引方案接口，返回string类型
	public String recommend_plan() {

		String s = readJsonFile("/home/gaomeng/smart-index/results");
		JSONObject jobj = JSON.parseObject(s);
		String plan = "We made index recommendations for 269KB data sets and 45KB workload."+"\n"+
				"The smart recommendation results are as follows:"+"\n"+
				"“uid”: Hash (fillfactor = 55)"+"\n"+
				"“peoples”:no index "+"\n"+
				"The results of manual recommendation are as follows:"+"\n"+
				"“uid”: Hash (fillfactor = 100)"+"\n"+
				"“peoples”:no index"+"\n"+
				"The results of the default scheme are as follows:"+"\n"+
				"“uid”: LSM"+"\n"+
				"“peoples”:no index"+"\n"+
				"The manual method, smart method, and default method took 129ms, 119ms, and 165ms respectively. The performance of intelligent methods is 8% higher than that of manual methods.";

		return plan;
	}
    
	// effencicy 接口
	public String recommend_effencicy() {

		String s = readJsonFile("/home/gaomeng/smart-index/results");
		JSONObject jobj = JSON.parseObject(s);
		String eff = jobj.getDouble("effencicy").toString();
		return eff;
	}

	public ArrayList<String> multi_modal(String sql) throws SQLException, ClassNotFoundException {
		String quStr = sql.substring(sql.indexOf("(") + 1, sql.indexOf(")"));
		String newstring = "Select * from userlike where userlike.uid in (" + quStr + ");";
		Connection c = null;
		Statement stmt = null;
		Class.forName("org.postgresql.Driver");
		String url = "jdbc:postgresql://127.0.0.1:" + port + "/" + dbname;
		c = DriverManager.getConnection(url, user, password);
		stmt = c.createStatement();
		ResultSet rs = stmt.executeQuery(newstring);
		ArrayList<String> relist = new ArrayList<String>();
		while (rs.next()) {
			String res = rs.getString("uid") + " " + rs.getString("peoples");
			relist.add(res);
		}
		stmt.close();
		c.close();
		return relist;
	}

	public ArrayList<String> kv_modal(String sql) throws SQLException, ClassNotFoundException {
		Connection c = null;
		Statement stmt = null;
		Class.forName("org.postgresql.Driver");
		String url = "jdbc:postgresql://127.0.0.1:" + port + "/" + dbname;
		Class.forName("org.postgresql.Driver");
		c = DriverManager.getConnection(url, user, password);
		stmt = c.createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		ArrayList<String> relist = new ArrayList<String>();
		while (rs.next()) {
			String res = rs.getString("uid") + " " + rs.getString("peoples");
			relist.add(res);
		}
		stmt.close();
		c.close();
		return relist;
	}

	public String jugeindex(Integer index) {
		if (index == 0) {
			return "Btree";
		} else if (index == 1) {
			return "Hash";
		} else {
			return "LSM";
		}

	}

	public String readFile(String fileName) {
		BufferedReader br;
		StringBuilder sb = new StringBuilder();
		try {
			br = new BufferedReader(new FileReader(new File(fileName)));
			String s;
			while ((s = br.readLine()) != null) {
				sb.append(s + "\n");
			}
			br.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return sb.toString();
	}

	public void writeFile(String fileName, String txt) {
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(fileName)));
			bw.write(txt);
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static String readJsonFile(String fileName) {
		String jsonStr = "";
		try {
			File jsonFile = new File(fileName);
			FileReader fileReader = new FileReader(jsonFile);
			Reader reader = new InputStreamReader(new FileInputStream(jsonFile), "utf-8");
			int ch = 0;
			StringBuffer sb = new StringBuffer();
			while ((ch = reader.read()) != -1) {
				sb.append((char) ch);
			}
			fileReader.close();
			reader.close();
			jsonStr = sb.toString();
			return jsonStr;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	public static void main(String[] args) {
		kv_modal cKv_modal = new kv_modal();
		System.out.println(cKv_modal.recommend_effencicy());
	}

}
