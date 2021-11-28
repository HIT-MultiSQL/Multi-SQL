package com.hit.backup.select;

import java.text.ParseException;
import java.util.*;
import java.io.*;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import com.hit.backup.select.execute;
import com.hit.backup.graph.Graph;
import com.hit.backup.graph.GraphTest;
import com.hit.backup.doc.*;
import com.hit.backup.kv.*;
import com.hit.backup.rel.*;

public class multimain {
	private static List<String> truelist = new ArrayList<String>();
	private static List<String> array = new ArrayList<>();
	private static List<String> sqllist = new ArrayList<String>();
	private static List<String> sqloplist = new ArrayList<String>();
	private static List<String> retList = new ArrayList<String>();
	private static List<String> joinres = new ArrayList<String>();
	private static List<String> reslist = new ArrayList<String>();
	private static String sql;
	static String filename = "/randworkload.txt";
	static String storename;
	//private static List<String> finalsql = new ArrayList<String>();
	
	
	private List<String> getList(String sql){
		execute ext = new execute();
		//ext.resolve(sql);
		int length = ext.resolve(sql).size();
		System.out.println(length);
		System.out.println(ext.resolve(sql).get(0));
		for(int i = 0; i < length; i++) {
			sqllist.add(ext.resolve(sql).get(i).trim());
		}
		System.out.println(sqllist);
		return sqllist;
	}
	
	
	private List<String> sortList(List<String> sqllist2) {
		retList.clear();
	    int size = sqllist2.size();
	    int listMaxSize = size;
	    while (retList.size() < listMaxSize) {
	        int maxLen = 0;
	        int maxIndex = 0;
	        for (int i = 0; i < size; i++) {
	            String str = sqllist2.get(i);
	            int len = 0;
	            if (str != null) {
	                len = str.length();
	            }
	            if (len > maxLen) {
	                maxLen = len;
	                maxIndex = i;
	            }
	        }
	        retList.add(sqllist2.get(maxIndex));
	        sqllist2.remove(maxIndex);
	        size--;
	    }
	    return retList;
	}
	
	public boolean isContains(String lineText,String word){
        Pattern pattern=Pattern.compile(word,Pattern.CASE_INSENSITIVE);
        Matcher matcher=pattern.matcher(lineText);
        return matcher.find();
    }	
	
	public List<String> toArrayByFileReader1(String name) {
		// 使用ArrayList来存储每行读取到的字符串
		ArrayList<String> arrayList = new ArrayList<>();
		try {
			FileReader fr = new FileReader(name);
			BufferedReader bf = new BufferedReader(fr);
			String str;
			// 按行读取字符串
			while ((str = bf.readLine()) != null) {
				arrayList.add(str);
			}
			bf.close();
			fr.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		// 对ArrayList中存储的字符串进行处理
		int length = arrayList.size();
		
		for (int i = 0; i < length; i++) {
			String s = arrayList.get(i);
			array.add(s);
		}
		// 返回数组
		return array;
	}
	
	public void writeFile(List<String> strlist){
	    try {
	        File writeName = new File(storename);
	        writeName.createNewFile();
	        try (FileWriter writer = new FileWriter(writeName, true);
	              BufferedWriter out = new BufferedWriter(writer)){
	        	for(int i = 0; i < strlist.size(); i++) {
	        		out.write(strlist.get(i));
	        		out.newLine();
	        	}
	        }
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
	}

	public int getSubCount(String str,String key){
		int count = 0;
		int index = 0;
		str = str.toLowerCase();
 
		while((index= str.indexOf(key,index))!=-1){
			//System.out.println("index="+index);
			index = index + key.length();
			count++;
		}
		return count;
	}
	
	//有关替换
	public List<String> truesql(List<String> list){
		truelist.clear();
		for(int i = 0; i < list.size(); i++) {
			String s1 = list.get(i);
			int num;
			num = getSubCount(s1,"select")+ getSubCount(s1,"join");
			if(num > 1) {
				for(int j = i+1; j < list.size(); j++) {
				String s2 = list.get(j);
					if(s1.contains(s2)) {
						String[] s = s2.split(" ");
						if(s[1].contains("gra")) {
							s1 = s1.replace(s2, "GraphTest.multiModalTrial");
						}
						else if(s[1].contains("kv")) {
							s1 = s1.replace(s2, "Kv_Test.multi_modal");
						}
						else if(s[1].contains("document")) {
							s1 = s1.replace(s2, "Document.getMultiQuery");
						}
						else if(s[1].contains("rel") || s[1].contains("activity")) {
							s1 = s1.replace(s2, "Execution.multiModeQuery");
						}
						else {
							s1 = s1.replace(s2, "cache.exe");
						}						
					}
					int cn = getSubCount(s1,"select")+ getSubCount(s1,"join");
					if(cn == 1) {
						truelist.add(s1);
						break;
					}					    
				}
			}				
			else {
				truelist.add(s1);
			}				
		}						
		return truelist;
	}
	
	public List<String> joinsameid(List<String> id1, List<String> id2){
		id1.retainAll(id2);
		for(int i = 0; i < id1.size(); i++){
			joinres.add(id1.get(i));
		}
		return joinres;
	}
	
	public String tolist(List<String> list) {
		return "'"+  String.join("','", list)+"'";
	}
	
	public String tolistscp(List<String> list) {
		return "("+"'"+  String.join("','", list)+"'"+")";
	}
		
	public List<String> res(List<String> finalsql) throws ClassNotFoundException, IOException, SQLException, ParseException {
		//new GraphTest();
		//GraphTest gratest = new GraphTest();
		Graph gratest = new Graph();
		Document doctest = new Document();
		Kv_Test kvtest = new Kv_Test();
		Execution reltest = new Execution();
		String joingra = null;
		String joinrel = null;
		
		List<String> kvlist = new ArrayList<String>();
		List<String> rellist = new ArrayList<String>();
		List<String> gralist = new ArrayList<String>();
		List<String> doclist = new ArrayList<String>();
		List<String> joinlist = new ArrayList<String>();
		
		
		
		for(int i = finalsql.size()-1; i >= 0 ; i--) {
			String s[] = finalsql.get(i).split(" ");
			System.out.println(finalsql.get(i));
			System.out.println(s[1]);
			if(!finalsql.get(i).contains("GraphTest.multiModalTrial") && !finalsql.get(i).contains("Document.getMultiQuery")&&!finalsql.get(i).contains("Kv_Test.multi_modal")&&!finalsql.get(i).contains("Execution.multiModeQuery")&&!finalsql.get(i).contains("cache.exe")) {
				if(s[1].contains("gra") && s[0].contains("select")) {
					gralist = gratest.graphQuery(finalsql.get(i));
				}
				else if(s[1].contains("gra") && s[0].contains("join")) {
					joingra = finalsql.get(i);
				}
				else if(s[1].contains("rel") && s[0].contains("Join")) {
					joinrel = finalsql.get(i);
					System.out.println(joinrel);
				}
				else if(s[1].contains("kv")) {
					kvlist = kvtest.multi_modal(finalsql.get(i));
				}
				else if((s[1].contains("rel") || s[i].contains("activity")) && s[0].contains("select")){
					rellist = reltest.multiModeQuery(finalsql.get(i),2);
				}
				else if(s[1].contains("document")) {
					System.out.println(finalsql.get(i));
					doclist = doctest.getMultiQuery(finalsql.get(i),2);
					System.out.println(doclist);
				}
			}
			else {
				if(s[0].contains("Join") && s[1].contains("sameid")) {
					joinlist.addAll(joinsameid(rellist, doclist));
					System.out.println(joinlist);
				}
				else if(s[0].contains("Select") && s[1].contains("sameid")) {
					continue;
				}
				else if(s[0].contains("Select") && s[1].contains("document") && finalsql.get(i).contains("Execution.multiModeQuery")) {
					String str = rellist.toString().replace(" ", "");
					String s0 = finalsql.get(i).replace("(Execution.multiModeQuery)", str);
					System.out.println(s0);
					doclist.addAll(doctest.getMultiQuery(s0,2));
					System.out.println(doclist);
				}
				else if(s[0].contains("Select") && s[1].contains("kv") && finalsql.get(i).contains("cache.exe")) {
					String str = tolist(joinlist);
					String s0 = finalsql.get(i).replace("cache.exe", str);
					kvlist.addAll(kvtest.multi_modal(s0));
				}
		
				else if(s[0].contains("Select") && s[1].contains("kv") && finalsql.get(i).contains("GraphTest.multiModalTrial")) {
					String str = tolist(gralist);
					String s0 = finalsql.get(i).replace("GraphTest.multiModalTrial", str);
					kvlist.addAll(kvtest.multi_modal(s0));
				}
				else if(s[0].contains("Select") && s[1].contains("kv") && finalsql.get(i).contains("Document.getMultiQuery")) {
					String str = tolist(doclist);
					String s0 = finalsql.get(i).replace("Document.getMultiQuery", str);
					kvlist.addAll(kvtest.multi_modal(s0));
				}
				else if(s[0].contains("Select") && s[1].contains("activity") && finalsql.get(i).contains("GraphTest.multiModalTrial")) {
					String str = gralist.toString().replace(" ", "");
					String s0 = finalsql.get(i).replace("(GraphTest.multiModalTrial)", str);
					System.out.println(s0);
					rellist.addAll(reltest.multiModeQuery(s0,0));
				}
				else if(s[0].contains("Select") && s[1].contains("rel") && finalsql.get(i).contains("GraphTest.multiModalTrial")) {
					String str = gralist.toString().replace(" ", "");
					String s0 = finalsql.get(i).replace("(GraphTest.multiModalTrial)", str);
					System.out.println(s0);
					rellist.addAll(reltest.multiModeQuery(s0,0));
				}
				
				else if(s[0].contains("Select") && s[1].contains("rel") && finalsql.get(i).contains("Execution.multiModeQuery")) {
					String s0 = finalsql.get(i).replace("Execution.multiModeQuery", joinrel);
					System.out.println(s0);
					rellist.addAll(reltest.multiModeQuery(s0,0));
					System.out.println(rellist);
				}
				else if(s[0].contains("Select") && s[1].contains("gra") && finalsql.get(i).contains("GraphTest.multiModalTrial")) {
					//String str = tolistscp(gralist);
					String s0 = finalsql.get(i).replace("GraphTest.multiModalTrial", joingra);
					System.out.println(s0);
					gralist.addAll(gratest.graphQuery(s0));
					System.out.println(gralist);
				}				
			}
		}
		
		String[] scut = finalsql.get(0).split(" ");
		if(scut[1].contains("activity") || scut[1].contains("rel")) {
			reslist = rellist;
		}
		else if(scut[1].contains("kv")) {
			reslist = kvlist;
		}
		else if(scut[1].contains("gra")) {
			reslist = gralist;
		}
		else if(scut[1].contains("doc")) {
			reslist = doclist;
		}
		return reslist;
	}


	public ArrayList<String> uploadworkload(String filepath){
		ArrayList<String> output = new ArrayList<String>();
		return output;
	}

	public long multiop(String sql) throws ClassNotFoundException, SQLException, ParseException, IOException {
		ArrayList<String> opresults = new ArrayList<String>();
		long starttime = System.currentTimeMillis();
		getList(sql);
		sortList(sqllist);
		truesql(retList);
		res(truelist);
		long endtime = System.currentTimeMillis();
		long time = endtime-starttime;
		System.out.println(time);

		return time;
	}

	public ArrayList<String> execution(String sql) throws ClassNotFoundException, SQLException, ParseException, IOException {
		//ArrayList<String> results = new ArrayList<String>();
		long starttime = System.currentTimeMillis();
		getList(sql);
		//System.out.println(sqllist);
		sortList(sqllist);
		//System.out.println(retList);
		if(retList.size() > 1){
			truesql(retList);
		}
		else{
			truelist.clear();
			truelist.addAll(retList);
		}
		//System.out.println(truelist);
		res(truelist);
		System.out.println(reslist);
		ArrayList<String> results = new ArrayList<String>(reslist);
		long endtime = System.currentTimeMillis();
		long time = endtime-starttime;
		System.out.println(time);

		return results;
	}

	
	public static void main(String[] args) throws ClassNotFoundException, IOException, SQLException, ParseException{
		
		
		//execute ext = new execute();
//		String sql1 = "Select kv.set where kv.id in (Select document.id where document.date between 20190516 and 20191004 and document.id in (Select rel.id Join rel (id = r1.id, register_date = register_date, log_time = log_time) r1, r2 r1.id = r2.id where register_date < 20010101 and log_time = 20100417))";
//		String sql2 = "Select kv.set where kv.id in (Select sameid.id Join sameid (id = rel.id) Select rel.id Join rel (id = r1.id, register_date = register_date, log_time = log_time) r1, r2 r1.id = r2.id where register_date < 20010101 and log_time = 20100417, Select document.id where document.date between 20190516 and 20191004 rel.id = document.id)";
		//sql = "Select kv.set where kv.id in (Select rel.id Join rel (id = r1.id, register_date = register_date, log_time = log_time) r1, r2 r1.id = r2.id where rel.id in(Select gro.a where gro.a in (Select document.id where document.date between 20181116 and 20190501 rel.id = document.id) and gro.p = follow and gro.o = “03165”) and register_date < 20120101 and log_time >= 20200701)";
		//sql = "Select kv.set where kv .id in (Select gra.s join gra (s = a.s, o = a.o, p = a.p, bp = b.p, bo = b.o, cp = c.p, co = c.o ) a, b, c a.s = b.s and b.p = c.s where gra.p = ��type�� and gra.o = ��senior user�� and gra.bp = ��comment�� and gra.cp = ��topic�� and gra.co = ��collage��)";
		//sql = "Select activity.device, count(activity.device) as device_count where log_time between 20200701 and 20200707 and uid in (Select gra.s, gra.eo, gra.fo join gra (s = a.s, o = a.o, p = a.p, bp = b.p, bo = b.o, cp = c.p, co = c.o, dp = d.p, do = d.o, ep = e.p, eo = e.o, fp = f.p, fo = f.o) a, b, c, d, e, f a.s = b.s and b.o = c.s and a.s = d.s and a.s = e.s and a.s = f.s where gra.p = 'follow' and gra.o = '06971' and gra.bp = 'publish' and gra.cp = 'topic' and gra.co= 'University' and gra.dp = 'gender' and gra.do = 'female' and gra.ep = 'occupation' and gra.fp = 'name') group by device;";
		//sql = "Select kv.set where kv.id in (Select sameid.id Join sameid (id = rel.id) Select rel.id Join rel (id = r1.id, register_date = register_date, log_time = log_time) r1, r2 r1.id = r2.id where register_date < 20120101 and log_time = 20200720, Select document.id where document.month between [M-1] and [M] rel.id = document.id)";
		//sql = "Select activity.device, count(activity.device) as device_count where log_time between [t] and [t-7d] and uid in (Select gra.s Join gra (s = a.s, o = a.o, p = a.p, bp = b.p, bo = b.o, cp = c.p, co = c.o, dp = d.p, do = d.o) a, b, c, d a.s = b.s and b.p = c.s and a.s = d.s where gra.p = ��follow�� and gra.o = ��someID�� and gra.bp = ��publish�� and gra.cp = ��topic�� and gra.co = ��mobile phone�� and gra.dp = ��gender�� and gra.do = ��female��) group by device";
		//sql = "Select activity.device, count(activity.device) as device_count where log_time between 20200701 and 20200707 and uid in (Select gra.s join gra (s = a.s, o = a.o, p = a.p, bp = b.p, bo = b.o, cp = c.p, co = c.o, dp = d.p, do = d.o) a, b, c a.s = b.s and b.p = c.s and a.s = d.s where gra.p = 'follow' and gra.o = '00972' and gra.bp = 'publish' and gra.cp = 'topic' and gra.co = '��ѧ' and gra.dp = 'gender' and gra.do = 'female') group by device";
		//ext.resolve(sql);		
		//sql = "Select kv.set where kv.id in (Select document.id where document.date = 20190222 and document.id in (select id Join sameid (id = rel.id) Select rel.id where rel.age between 20 and 30))";
		//sql = "Select kv.set where kv.id in (Select document.id where document.date = 20190222 and document.id in (Select rel.id where rel.age between 20 and 30))";
		//sql = "Select kv.set where kv.id in (Select document.id where document.date between [M-1] and [M] and document.id in (Select rel.id Join rel (id = r1.id, register_date = register_date, log_time = log_time) r1, r2 r1.id = r2.id where register_date < 20120101 and log_time> 20200712))";
		multimain op = new multimain();


//		long before = op.multiop(sql2);
//		long after = op.multiop(sql1);
//
//		long result = (before-after)/(before*100);
//
//		System.out.println(result);
//
//
//		long starttime = System.currentTimeMillis();
//		op.getList(sql);
//		op.sortList(sqllist);
//		for(int i = 0; i < retList.size(); i++) {
//			System.out.println(retList.get(i));
//		}
//		System.out.print("\n");
//		op.truesql(retList);
//		for(int i = 0; i < truelist.size(); i++) {
//			System.out.println(truelist.get(i));
//		}
//		long endtime = System.currentTimeMillis();
//		String time = (endtime-starttime)+"ms";
//		System.out.println(time);


		//op.execution("Select rel.id Join rel (id = r1.id, register_date = register_date, log_time = log_time) r1, r2 r1.id = r2.id where register_date < 20120101 and log_time = 20200720");



		ArrayList<String> timecost = new ArrayList<>();
		op.toArrayByFileReader1(filename);
		long stime = System.currentTimeMillis();
		for(int i = 0; i < array.size(); i++) {
			long starttime = System.currentTimeMillis();
			//System.out.println(array.get(i));
			op.getList(array.get(i));
			//System.out.println(sqllist.size());
			op.sortList(sqllist);
//			for(int j = 0; j < retList.size(); j++) {
//				//System.out.println(retList.get(j));
//				finalsql.add(retList.get(j));
//			}
			System.out.println(retList);
			op.truesql(retList);
			System.out.println(truelist);
			op.res(truelist);
			System.out.println(reslist);
			storename = "/home/wangyutong/"+"resdata"+i+".txt";
			op.writeFile(reslist);
			long endtime = System.currentTimeMillis();
			String time = (endtime-starttime)+"ms";
			timecost.add(time);
			System.out.println(time);
		}
		long etime = System.currentTimeMillis();
		System.out.println((etime-stime)+"ms");
		System.out.println(timecost);
		
		
	}

}
