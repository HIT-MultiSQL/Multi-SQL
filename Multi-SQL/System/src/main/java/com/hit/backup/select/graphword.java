package com.select_exe;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class graphword {
	
	private static String filename = "src/data/sqlquery.txt";
	private static List<String> array = new ArrayList<>();
	private static List<String> graphl = new ArrayList<>();
	
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
	
	public static List<String> graphlist(List<String> strlist){
		String regex = "(gra)(.+)";
		String regex1 = "(gra)";
		Pattern pattern=Pattern.compile(regex,Pattern.CASE_INSENSITIVE);
		Pattern pattern1=Pattern.compile(regex1,Pattern.CASE_INSENSITIVE);
		
		for(int i = 0; i < strlist.size(); i++) {			
			String[] s = null;
			s = strlist.get(i).split(" ");
			System.out.println(s[1]);
			Matcher matcher=pattern.matcher(s[1]);
			Matcher matcher1=pattern1.matcher(s[1]);
			while(matcher.find() || matcher1.find()) {
				graphl.add(strlist.get(i));
			}			
		}
		return graphl;
	}
	
	public static void writeFile(List<String> strlist){
	    try {
	        File writeName = new File("src/data/graphquery.txt");
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

	public ArrayList<String> ooo(String sql){
		ArrayList<String> dates = new ArrayList<String>();
		if(!sql.contains("in") && sql.contains("between")){

			String date1 = sql.substring(sql.indexOf("between") + 8, sql.indexOf("between")+16);
			String date2 = sql.substring(sql.indexOf("between") + 21, sql.indexOf("between")+29);
			dates.add(date1);
			dates.add(date2);

//			if(m.find()) {
//				sql = m.group(1) + " FROM " + table + " " + m.group(2);
//			}
//			else
//				throw new IllegalArgumentException("参数非法");
		}
		else if(!sql.contains("in") && !sql.contains("between")){
			String date = sql.substring(sql.indexOf("=") + 2, sql.indexOf("=")+10);
			dates.add(date);
		}

		else {
			String date1 = sql.substring(sql.indexOf("[") - 41, sql.indexOf("[") - 33);
			String date2 = sql.substring(sql.indexOf("[") - 28, sql.indexOf("[") - 20);
			dates.add(date1);
			dates.add(date2);
		}
		return dates;
	}


	public static void main(String[] args) {

		String str = "Select document.id where document.date = 20090823";
		String str2 = "Select document.id where document.date between 20200607 and 20200707";
		String str3 = "Select document.id where document.date between 20190516 and 20191004 and document.id in [id1,id2,id3]";

		graphword test = new graphword();
		System.out.println(test.ooo(str));
		System.out.println(test.ooo(str2));
		System.out.println(test.ooo(str3));

//		graphword gp = new graphword();
//		gp.toArrayByFileReader1(filename);
//		gp.graphlist(array);
//		gp.writeFile(graphl);
//		System.out.print(graphl.size());
		// TODO Auto-generated method stub

	}

}
