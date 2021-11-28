package com.hit.backup.doc;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.mongodb.*;


import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;



public class Document {

	private String dbname = "test";
	private String collectionDefault = "default";
	private String collectionArti = "arti";
	private String collectionMachine = "machine";
	private String name = "document";



	private String host = "localhost";
	//private String host = "172.17.0.2";
	private int port = 27017;


	/**
	 * 原子过滤
	 * @param sql select语句
	 * @return
	 * @throws UnknownHostException

	 */
//	private List<String> atomOperation(String sql, String table) throws UnknownHostException, ParseException {
//
//
//		MongoClient mongoClient = new MongoClient("172.17.0.2", 27017);
//		DB db = mongoClient.getDB(dbname);
//		DBCollection coll = db.getCollection(table);
//
//		Pattern r = Pattern.compile("(Select .*)from .* (where .*);");
//		Matcher m = r.matcher(sql);
//		if(m.find()) {
//			sql = m.group(1) + " FROM " + table + " " + m.group(2);
//		}
//		else
//			throw new IllegalArgumentException("参数非法");
//		sql = sql.replaceAll(name+"\\." , "");
//
//		//System.out.println(sql);
//		sql = addStringLogo(sql);
////		QueryConverter queryConverter = new QueryConverter.Builder().sqlString(sql).build();
////		MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
////		org.bson.Document query = mongoDBQueryHolder.getQuery();
////		org.bson.Document projection = mongoDBQueryHolder.getProjection();
//
////		BasicDBObject query1 = new BasicDBObject(query);
//		BasicDBObject query2 = new BasicDBObject("_id", 0);
//		query2.append("authorid",1);
//
//		//System.out.println(query1);
//		//System.out.println(query2);
//
////		DBCursor cursor = coll.find(query1,);
//		//DBCursor cursor = coll.find();
//		List<String> res = new ArrayList<String>();
//		try {
//			while(cursor.hasNext()) {
//				DBObject line = cursor.next();
//				String partRes = JsonHandle.jsonFormatter(line.toString());
//				JSONObject json = JSONObject.fromObject(partRes);
//
//				/*
//				for(Object k : json.keySet()){
//					Object v = json.get(k);
//				*/
//				res.add(json.get("id").toString());
//			}
//		}
//		finally {
////			cursor.close();
//		}
//		mongoClient.close();
//		return res;
//	}

	/**
	 * 原子过滤多模态
	 * @param sql select语句
	 * @return
	 * @throws UnknownHostException
	 * @throws ParseException
	 */
	private List<String> atomOperationMulti(String sql, String table) throws UnknownHostException, ParseException {
//		ServerAddress serverAddress = new  ServerAddress(host,port);
//		ArrayList<ServerAddress> addrs = new ArrayList<ServerAddress>();
//		addrs.add(serverAddress);
//		MongoCredential credential = MongoCredential.createScramSha1Credential(USER, "admin", PASS.toCharArray());
//		ArrayList<MongoCredential> credentials1 = new ArrayList<MongoCredential>();
//		credentials1.add(credential);

		//MongoClient mongoClient = new MongoClient(addrs,credentials1);
		MongoClient mongoClient = new MongoClient(host, 27017);
		DB db = mongoClient.getDB(dbname);
		DBCollection coll = db.getCollection(table);

		Pattern r = Pattern.compile("(Select .*)(where .*)");
		Matcher m = r.matcher(sql);
		BasicDBObject query1 = new BasicDBObject();
		if(!sql.contains("in") && sql.contains("between")){

			String date1 = sql.substring(sql.indexOf("between") + 8, sql.indexOf("between")+16);
			String date2 = sql.substring(sql.indexOf("between") + 21, sql.indexOf("between")+29);
			BasicDBList values = new BasicDBList();
			values.add(new BasicDBObject("date",new BasicDBObject("$gte",date1)));
			values.add(new BasicDBObject("date", new BasicDBObject("$lte",date2)));

			query1.put("$and", values);

//			if(m.find()) {
//				sql = m.group(1) + " FROM " + table + " " + m.group(2);
//			}
//			else
//				throw new IllegalArgumentException("参数非法");
		}
		else if(!sql.contains("in") && !sql.contains("between")){
			String date = sql.substring(sql.indexOf("=") + 2, sql.indexOf("=")+10);
			query1.append("date",date);
		}

		else if(sql.contains("in") && sql.contains("between")){
			String date1 = sql.substring(sql.indexOf("[")-41, sql.indexOf("[")-33);
			String date2 = sql.substring(sql.indexOf("[")-28, sql.indexOf("[")-20);
			String v = sql.substring(sql.indexOf("[") + 1, sql.indexOf("]"));
			String[] uids = v.split(",");
			BasicDBList values = new BasicDBList();
			for(int i = 0; i < uids.length; ++i){
				values.add(uids[i]);
			}
			BasicDBObject queryw = new BasicDBObject();
			queryw.append("authorid", new BasicDBObject("$in",values));
			BasicDBList values1 = new BasicDBList();
			values1.add(new BasicDBObject("date",new BasicDBObject("$gte",date1)));
			values1.add(new BasicDBObject("date", new BasicDBObject("$lte",date2)));

			BasicDBObject queryd = new BasicDBObject();
			queryd.put("$and",values1);

			BasicDBList valuew = new BasicDBList();
			valuew.add(queryw);
			valuew.add(queryd);
			query1.put("$and", valuew);


//			if(uids.length != 0){
//				StringBuilder sb = new StringBuilder();
//				sb.append("id = " + uids[0]);
//				int i = 1;
//				while (i < uids.length){
//					sb.append(" or id = "+uids[i]);
//					i++;
//				}
//				if(m.find()) {
//					sql = m.group(1) + "FROM " + table + " where document.date between "+ date1 + " and " + date2 + " and ("+sb.toString()+")";
//					System.out.println(sql);
//				}
//			}
		}
		else if(sql.contains("in") && !sql.contains("between")){
			String date = sql.substring(sql.indexOf("[")-28, sql.indexOf("[")-20);
			String v = sql.substring(sql.indexOf("[") + 1, sql.indexOf("]"));
			String[] uids = v.split(",");
			BasicDBList values = new BasicDBList();
			for(int i = 0; i < uids.length; ++i){
				values.add(uids[i]);
			}
			BasicDBObject queryw = new BasicDBObject();
			queryw.append("authorid", new BasicDBObject("$in",values));

			BasicDBObject queryd = new BasicDBObject();
			queryd.put("date",date);

			BasicDBList valuew = new BasicDBList();
			valuew.add(queryw);
			valuew.add(queryd);
			query1.put("$and", valuew);

		}

//		sql = sql.replaceAll(name+"\\." , "");
//
//		//System.out.println(sql);
//		sql = addStringLogo(sql);
//
//		QueryConverter queryConverter = new QueryConverter.Builder().sqlString(sql).build();
//		MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
//		org.bson.Document query = mongoDBQueryHolder.getQuery();
//		org.bson.Document projection = mongoDBQueryHolder.getProjection();


		BasicDBObject query2 = new BasicDBObject("_id", 0);
		query2.append("authorid",1);



		//System.out.println(query1.values());

		System.out.println(query1);
		System.out.println(query2);

		DBCursor cursor = coll.find(query1,query2);
		//DBCursor cursor = coll.find();
		List<String> res = new ArrayList<String>();
		try {
			while(cursor.hasNext()) {
				DBObject line = cursor.next();
				Object object = line.get("authorid");
				res.add(object.toString());
			}
		}
		finally {
			cursor.close();
		}
		mongoClient.close();
		return res;
	}

	private String addStringLogo(String sql) {
		Pattern r = Pattern.compile("(Select .* FROM .* where .* between) (\\d{8}) and (\\d{8})");
		Matcher m = r.matcher(sql);
		if(m.find()) {
			sql = m.group(1)+" \""+m.group(2)+"\""+" and "+"\""+m.group(3)+"\"";
		}
		r = Pattern.compile("(Select .* FROM .* where .* =) (\\d{8})");
		m = r.matcher(sql);
		if(m.find()) {
			sql = m.group(1)+" \""+m.group(2)+"\"";
		}
		//System.out.println(sql);
		return sql;
	}

//	private long testTime(String path, String table, List<Integer> num_list) throws UnknownHostException, ParseException {
//		List<String> sqls = new ArrayList<String>();
//		try {
//			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
//			String lineTxt = null;
//			while ((lineTxt = br.readLine()) != null)
//			{
//				sqls.add(lineTxt);
//			}
//			br.close();
//		} catch (Exception e) {
//			System.err.println("read errors :" + e);
//		}
//		long startTime = System.currentTimeMillis(); //获取开始时间
//		for(int i=0 ; i < 50;  i++)
//		{
//			atomOperation(sqls.get(num_list.get(i)), table);
//		}
//		long endTime = System.currentTimeMillis(); //获取结束时间
//		return endTime - startTime;
//	}
	/**
	 * 查询三种索引下（默认，人工方法，智能方法）一组查询的时间
	 * @return 时间的数组
	 * @throws UnknownHostException
	 * @throws ParseException
	 */
	public long[] genTime() throws UnknownHostException, ParseException {
		String path = "src/main/java/doc/new_workload";
		long[] timeList = new long[3];
		Random random = new Random(1);
		List<Integer> randomList = new ArrayList<Integer>();
		for(int i = 0;i<50;i++) {
			randomList.add(random.nextInt(20010));
		}
		//System.out.println(randomList);

//		timeList[0] = testTime(path, collectionDefault, randomList);
//		timeList[1] = testTime(path, collectionArti, randomList);
//		timeList[2] = testTime(path, collectionMachine, randomList);

		return timeList;
	}
//	/**
//	 * 单模态query查询
//	 * @param sql 查询语句
//	 * @return 查询结果id列表
//	 * @throws UnknownHostException
//	 * @throws ParseException
//	 */
//	public List<String> getSingleQuery(String sql, int select) throws UnknownHostException, ParseException{
//		switch(select) {
//			case 0:	return atomOperation(sql, this.collectionDefault);
//			case 1: return atomOperation(sql, this.collectionArti);
//			case 2:	return atomOperation(sql, this.collectionMachine);
//			default:return atomOperation(sql, this.collectionMachine);
//		}
//
//
//	}
	/**
	 * 多模态query查询
	 * @param sql
	 * @return 查询结果id列表
	 * @throws UnknownHostException
	 * @throws ParseException
	 */
	public List<String> getMultiQuery(String sql, int select) throws UnknownHostException, ParseException{
		switch(select) {
			case 0:	return atomOperationMulti(sql, this.collectionDefault);
			case 1: return atomOperationMulti(sql, this.collectionArti);
			case 2:	return atomOperationMulti(sql, this.collectionMachine);
			default:return atomOperationMulti(sql, this.collectionMachine);
		}

	}


}
