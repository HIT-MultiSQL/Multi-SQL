package com.hit.backup.select;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class execute {
	
	private static List<String> array = new ArrayList<>();
	private static String sql;
	private static final String Comma = ",";
	private static final String FourSpace = "    ";
	static boolean isSingleLine=true;
	private String selectcols;
	private String joincols;
	private final static List<String> sqllist = new ArrayList<String>();
	private static List<String> retList = new ArrayList<String>();
	private static List<String> truelist = new ArrayList<String>();
	static String filename = "src/data/randomworkload.txt";
	private static List<String> finallist = new ArrayList<String>();
	
	public String maxslp(String s) {
		if(isContains(s,"where") && isContains(s,"\\(")) {
			String regex0 = "(where)(.+)(\\()(.+)";
			String regex = "(?<=\\().*(?=\\))";
			Pattern pattern0=Pattern.compile(regex0,Pattern.CASE_INSENSITIVE);
			Pattern pattern=Pattern.compile(regex,Pattern.CASE_INSENSITIVE);
	        
	        Matcher matcher0=pattern0.matcher(s);
	        while(matcher0.find()) {
	        	Matcher matcher=pattern.matcher(matcher0.group());
	        	while(matcher.find()) {
	        		return matcher.group();
	        	}
	        }
		}
		return s;		
	}
	
	public static boolean isContains(String lineText,String word){
        Pattern pattern=Pattern.compile(word,Pattern.CASE_INSENSITIVE);
        Matcher matcher=pattern.matcher(lineText);
        return matcher.find();
    }
	
	public static int getSubCount(String str,String key){
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
	
	public List<String> resolve(String s) {
		sqllist.clear();
		String sql = s.trim();
		String temp = null;
		int m;
		sqllist.add(sql);
		
		//sql��where�������֣�����Ƕ�ײ��֣�
		if(!(sql.equals(maxslp(sql))) && !(maxslp(sql).equals(null))) {
			temp = maxslp(sql);
			sqllist.add(temp);
		}
		else {
			return sqllist;
		}
		
		int num;
		num = getSubCount(temp,"select")+ getSubCount(temp,"join");
		//System.out.println(num);
		
		//����select��where��Ƕ�ײ��ִ���
		while(num != 1) {
			//select���֣�����selectǶ��join�����
			if(temp.split("\\s+")[0].toLowerCase().equals("select")) {
				if(getSubCount(temp,"select")== getSubCount(temp, "where") && getSubCount(temp,"select") == 2) {
					temp = maxslp(temp);
				}
				else if(getSubCount(temp,"select")== getSubCount(temp, "where") && getSubCount(temp,"select") == 1 && isContains(temp,"join")) {
					String regex = "(join)(.+)(where)";
					Pattern pattern=Pattern.compile(regex,Pattern.CASE_INSENSITIVE);
					Matcher matcher=pattern.matcher(temp);
					while(matcher.find()) {
						temp = matcher.group(1)+matcher.group(2);
						
					}
				}
				else if(getSubCount(temp,"select") > getSubCount(temp, "where") && isContains(temp,"join")) {
					String regex1 = "(join)(.+)";
					Pattern pattern1=Pattern.compile(regex1,Pattern.CASE_INSENSITIVE);
					Matcher matcher1=pattern1.matcher(temp);
					while(matcher1.find()) {
						temp = matcher1.group();
					}
				}
				
			}
			//join���֣�����joinǶ��select�����
			else if(temp.split("\\s+")[0].toLowerCase().equals("join")) { 
				String regex2 = "(select)(.+)(,\\s+select)";
				String regex3 = "(,\\s+)(select)(.+)(\\s+)(.+)(=)";
				String tmp1;
				String tmp2;
				Pattern pattern2=Pattern.compile(regex2,Pattern.CASE_INSENSITIVE);
				Pattern pattern3=Pattern.compile(regex3,Pattern.CASE_INSENSITIVE);
				Matcher matcher2=pattern2.matcher(temp);
				Matcher matcher3=pattern3.matcher(temp);
				
				if(matcher2.find() && matcher3.find()) {
					if(!isContains(matcher3.group(),"join") && !isContains(matcher2.group(),"join")) {
						temp = matcher2.group(1)+matcher2.group(2);
						sqllist.add(matcher3.group(2)+matcher3.group(3));
					}
					else if(isContains(matcher3.group(),"join") && !isContains(matcher2.group(),"join")) {
						temp = matcher3.group(2)+matcher3.group(3);
						sqllist.add(matcher2.group(1)+matcher3.group(2));
					}
					else if(!isContains(matcher3.group(),"join") && isContains(matcher2.group(),"join")) {
						temp = matcher2.group(1)+matcher2.group(2);
						sqllist.add(matcher3.group(2)+matcher3.group(3));
					}
					
				}
				
//				while(matcher2.find()) {
//					if(isContains(matcher2.group(),"join")) {
//						temp = matcher2.group(1)+matcher2.group(2);
//					}
//					else {
//						sqllist.add(matcher2.group(1)+matcher2.group(2));
//					}
//				}
//				
//				while(matcher3.find()) {
//					if(isContains(matcher3.group(),"join")) {
//						temp = matcher3.group(2)+matcher3.group(3);
//					}
//					else {
//						sqllist.add(matcher3.group(2)+matcher3.group(3));
//					}
//				}
								
			}
			
			sqllist.add(temp.trim());
			num = getSubCount(temp,"select")+ getSubCount(temp,"join");
		}
		return sqllist;
	}
	
	
	
	private static List<String> sortList(List<String> sqllist2) {	
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
	
	public static List<String> toArrayByFileReader1(String name) {
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
	
	public static void writeFile(List<String> strlist){
	    try {
	        File writeName = new File("src/data/sqlquery.txt");
	        writeName.createNewFile();
	        try (FileWriter writer = new FileWriter(writeName, true);
	              BufferedWriter out = new BufferedWriter(writer)){
	        	for(int i = 0; i < strlist.size(); i++) {
	        		out.write(strlist.get(i));
	        		out.newLine();
	        	}
//	            out.write("写入文件1"); 
//	            out.newLine();   // 换行
//	            out.write("写入文件2");
//	            out.newLine();
//	            out.flush();
	        }
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
	}
	
	public static void main(String[] args){
		execute ext = new execute();
		//sql = "Select kv.set where kv.id in (Select document.id where document.year between [M-1] and [M] and document.id in (Select rel.id where rel.age between 20 and 30))";
		//sql = "Select kv.set where kv.id in (Select document.id where document.date between [M-1] and [M] and document.id in (Select rel.id Join rel (id = r1.id, register_date = register_date, log_time = log_time) r1, r2 r1.id = r2.id where register_date < 20120101 and log_time> 20200712))";
		//sql = "Select gra.s join gra (s = a.s, o = a.o, p = a.p, bp = b.p, bo = b.o, cp = c.p, co = c.o) a, b, c, d a.s = b.s and a.s = c.s where gra.p = ��follow�� and gra.o = ��someID�� and gra.bp = ��position�� and gra.bo = ��student�� gra.cp = ��type�� and gra.co = ��senior user�� )";
		//sql = "Select kv.set where kv.id in (Select gra.s join gra (s = a.s, o = a.o, p = a.p, bp = b.p, bo = b.o, cp = c.p, co = c.o ) a, b, c a.s = b.s and b.p = c.s where gra.p = ��type�� and gra.o = ��senior user�� and gra.bp = ��comment�� and gra.cp = ��topic�� and gra.co = ��collage��)";
		//sql = "/Select rel.id where rel.age between 20 and 30, Select document.id where document.year between [M-1] and [M] rel.id = document.id)";
		sql = "Select rel.id Join rel (id = r1.id, register_date = register_date, log_time = log_time) r1, r2 r1.id = r2.id where register_date < 20120101 and log_time = 20200720";
		//sql = "Select activity.device, count(activity.device) as device_count where log_time between [t] and [t-7d] and uid in (Select gra.s Join gra (s = a.s, o = a.o, p = a.p, bp = b.p, bo = b.o, cp = c.p, co = c.o, dp = d.p, do = d.o) a, b, c, d a.s = b.s and b.p = c.s and a.s = d.s where gra.p = ��follow�� and gra.o = ��someID�� and gra.bp = ��publish�� and gra.cp = ��topic�� and gra.co = ��mobile phone�� and gra.dp = ��gender�� and gra.do = ��female��) group by device";
//		ext.toArrayByFileReader1(filename);
//		for(int i = 0; i < array.size(); i++) {
//			System.out.println(array.get(i));
//			ext.resolve(array.get(i));
//			System.out.println(sqllist.size());
//			ext.sortList(sqllist);
//			for(int j = 0; j < retList.size(); j++) {
//				//System.out.println(retList.get(j));
//				finallist.add(retList.get(j));
//			}
//		}
//		
//		for(int k = 0; k < finallist.size(); k++) {
//			System.out.println(finallist.get(k));
//		}
//		ext.writeFile(finallist);
		
		ext.resolve(sql);
		ext.sortList(sqllist);
//
		System.out.println(sql);
		for(int i = 0; i < retList.size(); i++) {
			System.out.println(retList.get(i));
			String[] scut = retList.get(0).split(" ");
			System.out.println(scut[0]);
			//System.out.println();
		}
			
		
	}
}
