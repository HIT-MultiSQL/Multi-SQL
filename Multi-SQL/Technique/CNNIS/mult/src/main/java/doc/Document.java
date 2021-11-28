package doc;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.github.vincentrussell.query.mongodb.sql.converter.MongoDBQueryHolder;
import com.github.vincentrussell.query.mongodb.sql.converter.ParseException;
import com.github.vincentrussell.query.mongodb.sql.converter.QueryConverter;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import net.sf.json.JSONObject;


public class Document {
	
	private String dbname = "lastdemo";
	private String collectionDefault = "default";
	private String collectionArti = "arti";
	private String collectionMachine = "machine";
	private String name = "document";
	
	private String USER = "root";
	private String PASS = "123456";
	
	private String host = "localhost";
//	private String host = "127.17.17.4";
	private int port = 27017;
	
	
	/**
	 * 原子过滤
 	 * @param sql select语句
	 * @return
	 * @throws UnknownHostException
	 * @throws ParseException
	 */
	private List<String> atomOperation(String sql, String table) throws UnknownHostException, ParseException {
		ServerAddress serverAddress = new  ServerAddress(host,port);
		ArrayList<ServerAddress> addrs = new ArrayList<ServerAddress>(); 
		addrs.add(serverAddress);
	    MongoCredential credential = MongoCredential.createScramSha1Credential(USER, "admin", PASS.toCharArray()); 
	    ArrayList<MongoCredential> credentials1 = new ArrayList<MongoCredential>();
	    credentials1.add(credential);
	    
	    MongoClient mongoClient = new MongoClient(addrs,credentials1);	
		DB db = mongoClient.getDB(dbname);
		DBCollection coll = db.getCollection(table);
		
		Pattern r = Pattern.compile("(Select .*)from .* (where .*);");
	    Matcher m = r.matcher(sql);
	    if(m.find()) {
	    	sql = m.group(1) + " FROM " + table + " " + m.group(2);
	    }
	    else
	    	throw new IllegalArgumentException("参数非法");
	    sql = sql.replaceAll(name+"\\." , "");
	    
	    //System.out.println(sql);
	    sql = addStringLogo(sql);
		QueryConverter queryConverter = new QueryConverter.Builder().sqlString(sql).build();
		MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
		org.bson.Document query = mongoDBQueryHolder.getQuery();
		org.bson.Document projection = mongoDBQueryHolder.getProjection();
		
		BasicDBObject query1 = new BasicDBObject(query);
		BasicDBObject query2 = new BasicDBObject(projection);
		
		System.out.println(query1);
		System.out.println(query2);
		
		DBCursor cursor = coll.find(query1,query2);
		//DBCursor cursor = coll.find();
		
		List<String> res = new ArrayList<String>();
		try {
			while(cursor.hasNext()) {
				DBObject line = cursor.next();
				String partRes = JsonHandle.jsonFormatter(line.toString());
				JSONObject json = JSONObject.fromObject(partRes);
//			    System.out.println(json.toString());
				/*
				for(Object k : json.keySet()){    
					Object v = json.get(k);
				*/
				res.add(json.get("id").toString());    
			}
		} 
		finally {
		   cursor.close();
		}
		mongoClient.close();
        return res;
	}
	
	/**
	 * 原子过滤多模态
 	 * @param sql select语句
	 * @return
	 * @throws UnknownHostException
	 * @throws ParseException
	 */
	private List<String> atomOperationMulti(String sql, String table) throws UnknownHostException, ParseException {
		ServerAddress serverAddress = new  ServerAddress(host,port);
		ArrayList<ServerAddress> addrs = new ArrayList<ServerAddress>(); 
		addrs.add(serverAddress);
	    MongoCredential credential = MongoCredential.createScramSha1Credential(USER, "admin", PASS.toCharArray()); 
	    ArrayList<MongoCredential> credentials1 = new ArrayList<MongoCredential>();
	    credentials1.add(credential);
	    
	    MongoClient mongoClient = new MongoClient(addrs,credentials1);	
		DB db = mongoClient.getDB(dbname);
		DBCollection coll = db.getCollection(table);
		
		Pattern r = Pattern.compile("(Select .*)from .* (where .*)");
	    Matcher m = r.matcher(sql);
	    if(m.find()) {
	    	sql = m.group(1) + " FROM " + table + " " + m.group(2);
	    }
	    else
	    	throw new IllegalArgumentException("参数非法");
	    sql = sql.replaceAll(name+"\\." , "");
	    
	    //System.out.println(sql);
	    sql = addStringLogo(sql);
	    
		QueryConverter queryConverter = new QueryConverter.Builder().sqlString(sql).build();
		MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
		org.bson.Document query = mongoDBQueryHolder.getQuery();
		org.bson.Document projection = mongoDBQueryHolder.getProjection();
		
		BasicDBObject query1 = new BasicDBObject(query);
		BasicDBObject query2 = new BasicDBObject(projection);
		
		
			
		//System.out.println(query1.values());
		
		System.out.println(query1);
		System.out.println(query2);
		
		DBCursor cursor = coll.find(query1,query2);
		//DBCursor cursor = coll.find();
		List<String> res = new ArrayList<String>();
		try {
			while(cursor.hasNext()) {
				DBObject line = cursor.next();
				String partRes = JsonHandle.jsonFormatter(line.toString());
				JSONObject json = JSONObject.fromObject(partRes);
			       
				/*
				for(Object k : json.keySet()){    
					Object v = json.get(k);
				*/
				res.add(json.get("id").toString());    
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

	private long testTime(String path, String table, List<Integer> num_list) throws UnknownHostException, ParseException {
		List<String> sqls = new ArrayList<String>();
		try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
            String lineTxt = null;
            while ((lineTxt = br.readLine()) != null) 
            {
                sqls.add(lineTxt);
            }
            br.close();
        } catch (Exception e) {
            System.err.println("read errors :" + e);
        }
		long startTime = System.currentTimeMillis(); //获取开始时间
		for(int i=0 ; i < 50;  i++)
		{
			atomOperation(sqls.get(num_list.get(i)), table);
		}
		long endTime = System.currentTimeMillis(); //获取结束时间
		return endTime - startTime;
	}
	/**
	 * 查询三种索引下（默认，人工方法，智能方法）一组查询的时间
	 * @return 时间的数组
	 * @throws UnknownHostException
	 * @throws ParseException
	 */
	public long[] genTime() throws UnknownHostException, ParseException {
		String path = "src/main/java/doc/workload_dox";
		long[] timeList = new long[3];
		Random random = new Random(1);
		List<Integer> randomList = new ArrayList<Integer>();
		for(int i = 0;i<50;i++) {
			randomList.add(random.nextInt(20010));
		}
		//System.out.println(randomList);

		timeList[0] = testTime(path, collectionDefault, randomList);
		timeList[1] = testTime(path, collectionArti, randomList);
		timeList[2] = testTime(path, collectionMachine, randomList);
		
		return timeList;
	}
	/**
	 * 单模态query查询
	 * @param sql 查询语句
	 * @return 查询结果id列表
	 * @throws UnknownHostException
	 * @throws ParseException
	 */
	public List<String> getSingleQuery(String sql, int select) throws UnknownHostException, ParseException{
		switch(select) {
		case 0:	return atomOperation(sql, this.collectionDefault);
		case 1: return atomOperation(sql, this.collectionArti);
		case 2:	return atomOperation(sql, this.collectionMachine);
		default:return atomOperation(sql, this.collectionMachine);
		}
		
		
	}
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
	
	public static void main(String[] args) throws UnknownHostException, ParseException {
//		Document d = new Document();
//		long[] timelist = d.genTime();
//		for(long t:timelist) {
//			System.out.println(t);
//		}
		Document d = new Document();
		List<String> res = d.atomOperation("Select document.id from adb where document.date between 20190607 and 20200607;", d.collectionDefault);
//		for(String i:res) {
//			System.out.println(i);
//		}
		System.out.println(res.size());
	}
	
}
