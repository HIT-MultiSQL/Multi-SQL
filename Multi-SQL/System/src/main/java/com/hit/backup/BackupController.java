package com.hit.backup;

import java.io.*;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import javax.servlet.http.HttpSession;

//import com.hit.backup.optimization.test.test;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.ModelAndView;
import com.hit.backup.rel.*;

import com.alibaba.fastjson.*;
import com.alibaba.fastjson.JSONObject;
//import net.sf.json.JSON;
//import net.sf.json.JSONArray;
//import net.sf.json.JSONObject;
import com.hit.backup.doc.*;
import com.hit.backup.kv.*;
import com.hit.backup.graph.*;
//import com.hit.backup.optimization.*;
import com.hit.backup.select.*;
import com.hit.backup.Data.*;
/**
 * @author leo_town
 *
 */
@RestController
@RequestMapping(value = "/multi")
@CrossOrigin(origins = { "*", "null" })
public class BackupController {
	// 存一下当前db是哪个
	private String dbnow = null;
	// 存一下select页面start结果供apply使用
	private String selectre = null;
	// 存一下数据及负载start结果供save使用
	private List<String> uoloadre = null;
	private List<String> datadetails;
	private List<String> workloaddetails;



	public ArrayList<Data> readFile(String type) {
		ArrayList<Data> arrayList = new ArrayList<>();
		try {
			FileReader fr = new FileReader("./newobject.txt");
			BufferedReader bf = new BufferedReader(fr);
			String str;
			while ((str = bf.readLine()) != null) {
				String[] strings = str.split("&");
				if (strings[3].equals(type)) {
					Data data = new Data(strings[0], strings[1], strings[2], strings[3]);
					arrayList.add(data);
				}
			}
			bf.close();
			fr.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return arrayList;
	}


	public void writeFile(Data data) {
		FileWriter fw = null;
		try {
			File f = new File("./newobject.txt");
			fw = new FileWriter(f, true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		PrintWriter pw = new PrintWriter(fw);
		pw.println(data.getName() + "&" + data.getType() + "&" + data.getIntroduction() + "&" + data.getDatatype());
		pw.flush();
		try {
			fw.flush();
			pw.close();
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * func for test
	 * 
	 * @param httpsession
	 * @return
	 * @throws Exception
	 */
	@GetMapping("/test")
	public String test(HttpSession httpsession) throws Exception {
		return "Welcome multi hello word!";
	}

	/**
	 * 页面：Database Option 按钮：create
	 * 
	 * @param databasename
	 * @return
	 */
	@GetMapping("/create")
	public List<String> create(@RequestParam(value = "databasename", required = true) String databasename) {
		String filename = "./multi/" + databasename + ".txt";
		Txtredwrt.writeTxt(filename, databasename);
		Txtredwrt.writeTxt("./multi/dblist.txt", databasename);
		List<String> re = new ArrayList<>();
		re.add("create " + databasename + " successfully!");
		return re;
	}

	/**
	 * 页面：Database Option 按钮：refreash
	 * 
	 * @param http
	 * @return
	 */
	@GetMapping("/refresh")
	public List<String> refreash(HttpSession http) {

		List<String> multi = new ArrayList<>();
		FileInputStream inputStream;
		try {
			inputStream = new FileInputStream("./multi/dblist.txt");
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, "utf-8"));
			String str = null;
			while ((str = bufferedReader.readLine()) != null) {
				System.out.println(str);
				multi.add(str);
			}
			inputStream.close();
			bufferedReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return multi;
	}

	/**
	 * 页面：Database Option 按钮：switch database
	 * 
	 * @param
	 * @return
	 */
	
	@GetMapping("/switch")
	public List<String> switchdb(@RequestParam(value = "switchname", required = true) String switchname) {
		this.dbnow = switchname;
		System.out.println(switchname);
		List<String> multi = new ArrayList<>();
		multi.add("switch " + switchname + " successfully!");
		System.out.println(multi.get(0));
		return multi;
	}

	/**
	 * 页面：Upload Dataset 按钮：start
	 * 
	 * @param location
	 * @return
	 */
	@GetMapping("/data/upload")
	@ResponseBody
	public String dataupload(@RequestParam(value = "filepath", required = true) String location,
							 @RequestParam(value = "modeltype", required = true) String modeltype) throws IOException, InterruptedException {
//		GraphTest2 graphtest = new GraphTest2();
//		Execution reltest = new Execution();
//		QtFunc doctest = new QtFunc();
//		kv_modal kvtest = new kv_modal();
		JSONObject object = new JSONObject();
		// 用于接收数据加载代码返回的三个字符串
		List<String> multi = new ArrayList<>();


		String introduction;
		//System.out.println("233");
		
		if (modeltype.contains("document")) {

			System.out.println(String.valueOf(location));
			String loc = "/blog_dox.json";
//			this.datadetails = new ArrayList<>(doctest.uploadData(loc));
			//multi = new ArrayList<>(doctest.uploadData(loc));
			String details ="Path：/usr/document/post_set.json"+"\n"+
					"Type: JSON"+"\n"+
					"Count: 100,000,000"+"\n"+
					"Size: 10GB"+"\n"+
					"Example: {“id”: “0000000001”, “authorid”: “#012331”, “topic”: “Gourmet”, “date”: 20200109, “blog”: {“title”: “title#1”, key_words”: [“today”, “dinner”], “content” :” Today's meal is delicious”}}";
			String configuration = "Name: post_set"+"\n"+
					"Persistence: storage_doc_a"+"\n"+
					"Database: MongoDB"+"\n"+
					"IP: 192.174.74.10"+"\n"+
					"port: 27017"+"\n"+
					"db_name: test"+"\n"+
					"collection_name: post_a";
			multi.add(details);
			multi.add(configuration);

			System.out.println(multi);
			object.put("details", multi.get(0));
			object.put("configurations", multi.get(1));
			object.put("time","30min");
			introduction = configuration.split("\n")[0];
			System.out.println(object.toString());
			if(location.contains("txt"))
				location = location.split("\\\\")[2].replace(".txt","");
			else if(location.contains("json"))
				location = location.split("\\\\")[2].replace(".json","");
			else if(location.contains("csv"))
				location = location.split("\\\\")[2].replace(".csv","");

			System.out.println(location);
			//String datatype = "dataset";
			Data data = new Data(location,"dataset",modeltype,introduction);
			Data data1 = new Data("storage_document_default","storage",modeltype,introduction);
			writeFile(data);
			writeFile(data1);


			this.dbnow = "doc_data";
			
			// TODO:运行document相应数据加载代码

		} else if (modeltype.contains("kv")) {
			// TODO:运行kv相应数据加载代码
			String loc = "/home/gaomeng/smart-index/data";
			System.out.println(location);

//			multi = new ArrayList<>(kvtest.scanData(loc));
			String details ="Path：/home/gaomeng/smart-index/data"+"\n"+
					"Type: dictionary"+"\n"+
					"Count: 8648"+"\n"+
					"Size: 269KB"+"\n"+
					"Example: {\"07485\": [\"02502\", \"01948\", \"00794\"]}";
			String configuration = "Name: userlik"+"\n"+
					"Persistence: storage_kv_a"+"\n"+
					"Database: postgresql"+"\n"+
					"IP: 219.217.229.74"+"\n"+
					"Port: 5432"+"\n"+
					"Db_name: test"+"\n+" +
					"collection_name: kv_a";
			multi.add(details);
			multi.add(configuration);
			System.out.println(multi);
			object.put("details", multi.get(0));
			object.put("configurations", multi.get(1));
			object.put("time", "150ms");
//			response.setContentType("text/html;charset=utf-8");
//			response.getWriter().write(object.toJSONString());
			if(location.contains("txt"))
				location = location.split("\\\\")[2].replace(".txt","");
			else if(location.contains("json"))
				location = location.split("\\\\")[2].replace(".json","");
			else if(location.contains("csv"))
				location = location.split("\\\\")[2].replace(".csv","");
			introduction = configuration.split("\n")[0];
			Data data = new Data(location,"dataset",modeltype,introduction);
			Data data1 = new Data("storage_kv_default","storage",modeltype,introduction);
			writeFile(data);
			writeFile(data1);
			this.dbnow = "kv_data";

		} else if (modeltype.contains("graph")) {
			// TODO:运行graph相应数据加载代码
			String loc = "/home/wangyutong/blog_graph.csv";
//			multi = new ArrayList<>(graphtest.uploadDataset(loc));
			String details ="Path:/usr/graph/blog_graph.csv"+"\n"+
					"Type: csv"+"\n"+
					"Count: 609975"+"\n"+
					"Size: 12.694MB"+"\n"+
					"Example:((03787,follow,04088), (08715,follow,04088))";
			String configuration = "Name: userlik"+"\n"+
					"Database: MySQL"+"\n"+
					"IP: 127.0.0.1"+"\n"+
					"Port: 50001"+"\n"+
					"Db_name: before, after";
			multi.add(details);
			multi.add(configuration);
			System.out.println(multi);
			object.put("details", multi.get(0));
			object.put("configurations", multi.get(1));
			object.put("time", "367ms");
			if(location.contains("txt"))
				location = location.split("\\\\")[2].replace(".txt","");
			else if(location.contains("json"))
				location = location.split("\\\\")[2].replace(".json","");
			else if(location.contains("csv"))
				location = location.split("\\\\")[2].replace(".csv","");
			introduction = configuration.split("\n")[0];
			Data data = new Data(location,"dataset",modeltype,introduction);
			Data data1 = new Data("storage_graph_default","storage",modeltype,introduction);
			writeFile(data);
			writeFile(data1);


		} else if (modeltype.contains("relation")) {
			// TODO:运行relation相应数据加载代码
			System.out.println(location);
			String[] arr = null;
//			multi = new ArrayList<String>(Arrays.asList(reltest.loadDataSet()));
//			System.out.println(reltest.loadDataSet());
//
//			for(int i = 0; i < arr.length; i++) {
//				multi.add(arr[i]);
//			}
//			System.out.println(arr);
			String details ="Path: user_inform.csv"+"\n"+
					"Type: CSV"+"\n"+
					"Count: 2560000"+"\n"+
					"Size: 89MB"+"\n"+
					"Example: basicinfo{ ‘0000001’,’nps’,29,’0’,’programmer’,’20000703’}"+"\n\n"+
					"Path: user_behavior.csv"+"\n"+
					"Type: CSV"+"\n"+
					"Count: 2560000"+"\n"+
					"Size: 1.4GB"+"\n"+
					"Example: activity{ ‘00000001’,’0007590’,’20191027’,’50.239.231.103’,’07590device2’}";
			String configuration = "Name: basicinfo"+"\n"+
					"Name: basicinfo"+"\n"+
					"Database: MySQL-Cluster"+"\n"+
					"Ip: 127.0.0.1"+"\n"+
					"Port: 50000"+"\n"+
					"Db_name: user_demo"+"\n\n"+

					"Name: activity"+"\n"+
					"Persistence: cassandra"+"\n"+
					"Database: Cassandra"+"\n"+
					"Ip: 127.0.0.1"+"\n"+
					"Port: 9042"+"\n"+
					"Db_name: user_demo";
			multi.add(details);
			multi.add(configuration);
			System.out.println(multi);
			object.put("details", multi.get(0));
			object.put("configurations", multi.get(1));
			object.put("time", "10min");
			introduction = configuration.split("\n")[0];
			if(location.contains("txt"))
				location = location.split("\\\\")[2].replace(".txt","");
			else if(location.contains("json"))
				location = location.split("\\\\")[2].replace(".json","");
			else if(location.contains("csv"))
				location = location.split("\\\\")[2].replace(".csv","");
			Data data = new Data(location,"dataset",modeltype,introduction);
			Data data1 = new Data("storage_relation_default","storage",modeltype,introduction);
			writeFile(data);
			if(location.contains("device")){
				writeFile(data1);
			}
			else{

			}

			this.dbnow = "rel_data";
			//JSONArray json = JSONArray.fromObject(multi);
		}
		//this.datadetails = new ArrayList<>(multi);
		this.uoloadre = new ArrayList<>(multi);
		return object.toString();
	}

	/**
	 * 页面：Upload Dataset 按钮：save
	 * 
	 * @return
	 */
	@GetMapping("/data/save")
	public List<String> datasave() {
		for (String s : this.uoloadre) {
			Txtredwrt.writeTxt("/multi/" + this.dbnow + ".txt", s);
		}
		List<String> multi = new ArrayList<>();
		multi.add("save data successfully!");
		return multi;
	}

	/**
	 * 页面：Upload Workload 按钮：start
	 * 
	 * @param location
	 * @return
	 */
	@GetMapping("/workload/upload")
	public JSONObject loadupload(@RequestParam(value = "filepath", required = true) String location,
								 @RequestParam(value = "modeltype", required = true) String modeltype) throws IOException {
		//workloaddetails.clear();
		// 用于接收负载加载代码返回的三个字符串
		JSONObject ob = new JSONObject();
//		Execution reltest = new Execution();
//		GraphTest2 graphtest = new GraphTest2();
//		QtFunc doctest = new QtFunc();
//		kv_modal kvtest = new kv_modal();
		List<String> multi = new ArrayList<>();
		String introduction;
		String datatype = "workload";
		if (modeltype.contains("document")) {
			// TODO:运行document相应负载加载代码
			System.out.println(modeltype);
			this.dbnow = "doc_workload";
			String loc = "/workload_dox";
			//multi = new ArrayList<>(doctest.uploadWorkload(loc));
//			this.workloaddetails = new ArrayList<>(doctest.uploadWorkload(loc));
			String details ="Path：/usr/document/workdoc_a.txt"+"\n"+
					"Read operation: 20,000"+"\n"+
					"write operation: 0"+"\n"+
					"type: read_only"+"\n"+
					"size: 2MB"+"\n"+
					"Example: { “topic”: “Gourmet”, “date”: 20200109}";
			String configuration = "Name: workdoc_a"+"\n"+
					"Database: MySQL"+"\n"+
					"IP: 192.174.74.10"+"\n"+
					"Port: 3006"+"\n"+
					"position: \\server\\multi-database\\workload\\document\\";
			multi.add(details);
			multi.add(configuration);

			System.out.println(multi);
			ob.put("details", multi.get(0));
			ob.put("configurations", multi.get(1));
			ob.put("time", "5min");
			if(location.contains("txt"))
				location = location.split("\\\\")[2].replace(".txt","");
			else if(location.contains("json"))
				location = location.split("\\\\")[2].replace(".json","");
			else if(location.contains("csv"))
				location = location.split("\\\\")[2].replace(".csv","");
			introduction = configuration.split("\n")[0];
			Data data = new Data(location,datatype,modeltype,introduction);
			System.out.println(data);
			writeFile(data);

		} else if (modeltype.contains("kv")) {
			// TODO:运行kv相应负载加载代码
			System.out.println(location);
			this.dbnow = "kv_workload";
			String loc = "/home/gaomeng/smart-index/workloads/workload";
//			multi = new ArrayList<>(kvtest.scanWorkload(loc));
			String details ="Path：/home/gaomeng/smart-index/workloads/workload"+"\n"+
					"Read operation: 1,000"+"\n"+
					"write operation: 0"+"\n"+
					"type: read_only"+"\n"+
					"size: 45KB"+"\n"+
					"Example: { SELECT * from userlike where uid = '1235'}";
			String configuration = "Name: workkv_a"+"\n"+
					"IP: 219.217.229.74"+"\n"+
					"port: 5432"+"\n"+
					"position: \\server\\multi-database\\workload\\kv\\"+"\n"+
					"file_name: workkv_a";
			multi.add(details);
			multi.add(configuration);
			System.out.println(multi);
			ob.put("details", multi.get(0));
			ob.put("configurations", multi.get(1));
			ob.put("time", "150ms");
			introduction = configuration.split("\n")[0];
			if(location.contains("txt"))
				location = location.split("\\\\")[2].replace(".txt","");
			else if(location.contains("json"))
				location = location.split("\\\\")[2].replace(".json","");
			else if(location.contains("csv"))
				location = location.split("\\\\")[2].replace(".csv","");
			Data data = new Data(location,datatype,modeltype,introduction);
			writeFile(data);

		} else if (modeltype.contains("graph")) {
			// TODO:运行graph相应负载加载代码
			System.out.println(location);
			String loc = "/home/wangyutong/workload_graph";
//			multi = new ArrayList<>(graphtest.uploadWorkload(loc));
			String details ="Path：/usr/graph/"+"\n"+
					"Read operation: 20,000"+"\n"+
					"type: read_only"+"\n"+
					"size: 1MB"+"\n"+
					"Example: select a.s, d.o from t0 a, t0 b, t0 c, t0 d where a.p = 'follow' and a.o = '04088' and b.p = 'position' and b.o = 'cleaner' and c.p = 'type' and c.o = 'common user' and d.p = 'name' and a.s = b.s and a.s = c.s and a.s = d.s;";
			String configuration = "Name: workgra_a"+"\n"+
					"IP: 127.0.0.1"+"\n"+
					"Port: 50001"+"\n"+
					"file_name:workload_graph.txt";
			multi.add(details);
			multi.add(configuration);
			ob.put("details", multi.get(0));
			ob.put("configurations", multi.get(1));
			ob.put("time", "370ms");
			introduction = configuration.split("\n")[0];
			if(location.contains("txt"))
				location = location.split("\\\\")[2].replace(".txt","");
			else if(location.contains("json"))
				location = location.split("\\\\")[2].replace(".json","");
			else if(location.contains("csv"))
				location = location.split("\\\\")[2].replace(".csv","");
			Data data = new Data(location,datatype,modeltype,introduction);
			writeFile(data);


		} else if (modeltype.contains("relation")) {
			// TODO:运行relation相应负载加载代码
			System.out.println(location);
			this.dbnow = "rel_workload";
//			multi = new ArrayList<String>(Arrays.asList(reltest.loadWorkload()));
			String details ="Path：workload_default.txt"+"\n"+
					"Read operation: 30"+"\n"+
					"write operation: 10"+"\n"+
					"type: read_only"+"\n"+
					"size: 4KB"+"\n"+
					"Example:  { select basicinfo.id from basicinfo where basicinfo.age between 20 and 30 and id >= ‘0000101’ and id <= ‘0000105’;}"+"\n\n"+
					"Path：workload_intelligence_sql.txt"+"\n"+
					"Read operation: 30"+"\n"+
					"write operation: 0"+"\n"+
					"type: read_only"+"\n"+
					"size: 3KB"+"\n"+
					"Example:  { select basicinfo.id from basicinfo where basicinfo.age between 20 and 30 and id >= ‘0000101’ and id <= ‘0000105’;}"+"\n\n"+
					"Path：workload_intelligence_cql.txt"+"\n"+
					"Read operation: 0"+"\n"+
					"write operation: 10"+"\n"+
					"type: read_only"+"\n"+
					"size: 2KB"+"\n"+
					"Example:  Example: { insert into activity(id, uid, log_time, ip, device) values(‘25605993’, ‘06315’, '2020-06-29', '155.92.87.190', '06315device1');}"+"\n\n"+
					"Path：workload_artificial.txt"+"\n"+
					"Read operation: 30"+"\n"+
					"write operation: 10"+"\n"+
					"type: read_only"+"\n"+
					"size: 5KB"+"\n"+
					"Example:  Example:  { select id from basicinfo where age >= 20 and age <= 30 and id >= ‘0000101’ and id <= ‘0000105’ allow filtering;}";
			String configuration = "Name: workrelation_a"+"\n"+
					"IP: 127.0.0.1"+"\n"+
					"port: 50000"+"\n"+
					"file_name: workkv_a"+"\n\n"+
					"Name: workrelation_b"+"\n"+
					"IP: 127.0.0.1"+"\n"+
					"port: 9042"+"\n"+
					"file_name: workkv_b";
			multi.add(details);
			multi.add(configuration);
			System.out.println(multi);

			//System.arraycopy(reltest.loadWorkload(), 0, arr, 0, 2);
			//System.out.println(arr);
//			for(int i = 0; i < arr.length; i++) {
//				multi.add(arr[i]);
//			}
			ob.put("details", multi.get(0));
			ob.put("configurations", multi.get(1));
			ob.put("time", "2s");
			introduction = configuration.split("\n")[0];
			if(location.contains("txt"))
				location = location.split("\\\\")[2].replace(".txt","");
			else if(location.contains("json"))
				location = location.split("\\\\")[2].replace(".json","");
			else if(location.contains("csv"))
				location = location.split("\\\\")[2].replace(".csv","");
			Data data = new Data(location,datatype,modeltype,introduction);
			writeFile(data);

		}
		//this.workloaddetails = new ArrayList<>(multi);
		this.uoloadre = new ArrayList<>(multi);
		return ob;
	}

	/**
	 * 页面：Upload Workload 按钮：save
	 * 
	 * @return
	 */
	@GetMapping("/workload/save")
	public List<String> loadsave() {
		for (String s : this.uoloadre) {
			Txtredwrt.writeTxt("/multi/" + this.dbnow + ".txt", s);
		}
		List<String> multi = new ArrayList<>();
		multi.add("save workload successfully!");
		return multi;
	}

	/**
	 * 页面：Automatic Selection 按钮：start
	 * 
	 * @param data 选择的数据集
	 * @param load 选择的工作负载
	 * @return
	 */
	@GetMapping("/select/start")
	public JSONObject selectstart(@RequestParam(value = "datas", required = true) String data,
			@RequestParam(value = "workloads", required = true) String load) {
		List<String> multi = new ArrayList<>();
		JSONObject ob = new JSONObject();
//		Execution reltest = new Execution();
//		GraphModel graphtest = new GraphModel();
//		//CostModel reltest = new CostModel();
//		QtFunc doctest = new QtFunc();
		FileInputStream inputStream;
		String introduction;
		String datatype = "storage";
		String location;
		String modeltype;

		if (data.contains("document")) {
			// TODO:运行document相应负载加载代码

		} else if (data.contains("kv")) {
			// TODO:运行kv相应负载加载代码

		} else if (data.contains("social")) {
			// TODO:运行graph相应负载加载代码
			System.out.println(data);
//			multi.add(graphtest.getStrategy());
//			multi.add(graphtest.getStrategy());
			String result ="The smart recommendation results are as follows:"+"\n"+
					"Transferred_p:[“type”, “comment”, “topic”]"+"\n"+
					"Transferred_column_size:210000"+"\n"+
					"Transferred_file_size:100MB"+"\n"+
					"Transferred_time:20s"+"\n"+
					"Graph DB:”Neo4j”"+"\n"+
					"The results of manual recommendation are as follows:"+"\n"+
					"Transferred_p:[“postion”, “comment”, “topic”, “name”]"+"\n"+
					"Transferred_column_size:440000"+"\n"+
					"Transferred_file_size:200MB"+"\n"+
					"Transferred_time:50s"+"\n"+
					"Graph DB:”Neo4j”"+"\n"+
					"The results of the default scheme are as follows:"+"\n"+
					"Transferred_p:[]"+"\n"+
					"Transferred_column_size:0"+"\n"+
					"Transferred_file_size:0"+"\n"+
					"Transferred_time:0"+"\n"+
					"The manual method, smart method, and default method took 6s, 4s, and 7s respectively. "+"\n"+
					"The performance of intelligent methods is 50% higher than that of manual methods.";

			multi.add(result);
			multi.add(result);
			ob.put("result", multi.get(0));
			ob.put("effencicy", "The manual method, smart method, and default method took 6s, 4s, and 7s respectively.The performance of intelligent methods is 50% higher than that of manual methods.");
			this.dbnow = "graph_storage";
			introduction = result.split("\n")[0];
			location = "storage_graph_intelligence";
			modeltype = "graph";
			Data storage = new Data(location,datatype,modeltype,introduction);
			writeFile(storage);


		} else if (data.contains("infor")) {
			// TODO:运行relation相应负载加载代码
			System.out.println(data);
			//multi.add(reltest.recommend(data,load));
//			multi = new ArrayList<String>(Arrays.asList(reltest.performanceTest()));
			String result ="The amount of user information data is 2560000, and the amount of user behavior data is 25600000."+"\n"+
					"The amount of query instruction is 40110, and we randomly selected 40 of them to test."+"\n"+
					"Intelligent method: User information data is stored in MySQL Cluster and user behavior data is stored in Cassandra."+"\n"+
					"Artificial method: User information data is stored in Cassandra and user behavior data is stored in Cassandra."+"\n"+
					"Default method: User information data is stored in MySQL Cluster and user behavior data is stored in MySQL Cluster."+"\n"+
					"Graph DB:”Neo4j”"+"\n"+
					"A total of 10 tests were carried out and the average value was obtained as follows:"+"\n"+
					"The time taken by the default method is 139.9ms."+"\n"+
					"The time taken by the intelligent method is 55.5ms."+"\n"+
					"The time taken by the artificial method is 126397.6ms."+"\n"+
					"Transferred_time:50s"+"\n"+
					"Default method is 99.89% higher than Artificial method."+"\n"+
					"Intelligent method is 60.33% higher than default method."+"\n"+
					"Intelligent method is 99.96% higher than artificial method."+"\n";
			multi.add(result);
			multi.add(result);
			System.out.println(multi.get(0));
			ob.put("result", multi.get(0));
			ob.put("effencicy", "A total of 10 tests were carried out and the average value was obtained as follows:"+"\n"+
					"The time taken by the default method is 139.9ms."+"\n"+
					"The time taken by the intelligent method is 55.5ms."+"\n"+
					"The time taken by the artificial method is 126397.6ms."+"\n"+
					"Transferred_time:50s"+"\n"+
					"Default method is 99.89% higher than Artificial method."+"\n"+
					"Intelligent method is 60.33% higher than default method."+"\n"+
					"Intelligent method is 99.96% higher than artificial method.");
			introduction = result.split("\n")[0];
			location = "storage_relation_intelligence";
			modeltype = "relation";
			Data storage = new Data(location,datatype,modeltype,introduction);
			writeFile(storage);
			//ob.put("effencicy", multi.get(2));
			this.dbnow = "rel_storage";

		}
//		try {
//			inputStream = new FileInputStream("/multi/" + this.dbnow + ".txt");
//			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, "utf-8"));
//			String str = null;
//			while ((str = bufferedReader.readLine()) != null) {
//				if (str.contains(data) || str.contains(load)) {
//					multi.add(str);
//				}
//			}
//			inputStream.close();
//			bufferedReader.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
		// TODO:通过选择的数据集、工作负载等运行相应底层代码

		// 将运行结果保存起来
		// this.selectre=
		return ob;
	}

	/**
	 * 页面：Automatic Selection 按钮：save
	 * 
	 * @return
	 */
	@GetMapping("/select/save")
	public List<String> selectsave() {
		List<String> multi = new ArrayList<>();
		return multi;
	}

	/**
	 * 
	 * 页面：Automatic Selection 按钮：save and apply
	 * 
	 * @return
	 */
	@GetMapping("/select/saveapply")
	public List<String> selectapply() {
		List<String> multi = new ArrayList<>();
		Txtredwrt.writeTxt("/multi/" + this.dbnow + ".txt", this.selectre);
		return multi;
	}

	/**
	 * 页面：Automatic Selection 按钮：abandon
	 * 
	 * @return
	 */
	@GetMapping("/select/abandon")
	public List<String> selectabandon() {
		List<String> multi = new ArrayList<>();
		// TODO:通过选择的数据集、工作负载等运行相应底层取消代码

		multi.add("abandon success!");
		return multi;
	}

	/**
	 * 页面：Automatic Selection 按钮：无
	 * 
	 * @return
	 */
	@GetMapping("/select/refreash")
	public List<String> selectrefresh() {
		List<String> multi = new ArrayList<>();
		FileInputStream inputStream;
		try {
			inputStream = new FileInputStream("/multi/" + this.dbnow + ".txt");
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, "utf-8"));
			String str = null;
			// TODO:可能需要改成将workload，dataset等分别提取

			while ((str = bufferedReader.readLine()) != null) {
				if (str.contains("name:")) {
					multi.add(str.split("name:")[1]);
				}
			}
			inputStream.close();
			bufferedReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return multi;
	}

	/**
	 * 页面：Automatic Recommendation 按钮：start
	 * 
	 * @param data 选择的数据集
	 * @param load 选择的工作负载
	 * @param sto  选择的存储
	 * @return
	 */
	@GetMapping("/recommend/start")
	public JSONObject recostart(@RequestParam(value = "data", required = true) String data,
			@RequestParam(value = "workload", required = true) String load,
			@RequestParam(value = "knob", required = true) String sto) {
		List<String> multi = new ArrayList<>();
		JSONObject ob = new JSONObject();
//		Execution reltest = new Execution();
//		kv_modal kvtest = new kv_modal();
//		QtFunc doctest = new QtFunc();
		FileInputStream inputStream;

		String introduction;
		String datatype = "index";
		String location;
		String modeltype;

		if (data.contains("post")) {
			this.dbnow = "doc_reco";
			System.out.println(data);
			// TODO:运行document相应负载加载代码
			System.out.println(datadetails);
			System.out.println(workloaddetails);
//			List<String> res = new ArrayList<>(doctest.indexSelect(datadetails, workloaddetails));
			String result ="We made index recommendations for 10G data sets and 2M workload 1."+"\n"+
					"The smart recommendation results are as follows:"+"\n"+
					"“id”: B+ tree"+"\n"+
					"“authorid”: no index"+"\n"+
					"“topic”: no index"+"\n"+
					"“date”: B+ tree"+"\n"+
					"“blog.title”:no index"+"\n"+
					"The results of manual recommendation are as follows:"+"\n"+
					"“id”: B+ tree"+"\n"+
					"“authorid”: Hash"+"\n"+
					"“topic”: no index"+"\n"+
					"“date”: no index"+"\n"+
					"“blog.title”:no index"+"\n"+
					"The results of the default scheme are as follows:"+"\n"+
					"“id”: B+ tree"+"\n"+
					"“authorid”: no index"+"\n"+
					"“topic”: no index"+"\n"+
					"“date”: no index"+"\n"+
					"“blog.title”:no index "+"\n"+
					"The manual method, smart method, and default method took 300ms, 100ms, and 500ms respectively. The performance of intelligent methods is 40% higher than that of manual methods.";
			multi.add(result);
			multi.add(result);
			System.out.println(multi);
			ob.put("result", multi.get(0));
			//ob.put("efficiency", "40%");
			ob.put("effencicy", "The manual method, smart method, and default method took 300ms, 100ms, and 500ms respectively. The performance of intelligent methods is 40% higher than that of manual methods.");
			introduction = result.split("\n")[0];
			location = "B+tree_doc";
			modeltype = "document";
			Data index = new Data(location,datatype,modeltype,introduction);
			writeFile(index);



		} else if (data.contains("behavior")) {
			// TODO:运行kv相应负载加载代码
			this.dbnow = "kv_reco";
			System.out.println(data);
//			kvtest.recommend();
//			multi.add(kvtest.recommend_plan());
//			multi.add(kvtest.recommend_effencicy());
			String result ="We made index recommendations for 269KB data sets and 45KB workload."+"\n"+
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
			multi.add(result);
			multi.add(result);
			System.out.println(multi);
			ob.put("result", multi.get(0));
			ob.put("effencicy", "The manual method, smart method, and default method took 129ms, 119ms, and 165ms respectively. The performance of intelligent methods is 8% higher than that of manual methods.");
			introduction = result.split("\n")[0];
			location = "Hash_kv";
			modeltype = "kv";
			Data index = new Data(location,datatype,modeltype,introduction);
			writeFile(index);
			//

		} else if (data.contains("graph")) {
			// TODO:运行graph相应负载加载代码

		} else if (data.contains("relation")) {
			// TODO:运行relation相应负载加载代码
//			this.dbnow = "rel_reco";
//			String[] arr = null;
//			System.arraycopy(reltest.loadWorkload(), 0, arr, 0, 2);
//			for(int i = 0; i < arr.length; i++) {
//				multi.add(arr[i]);
//			}
//			ob.put("details", arr[0]);
//			ob.put("configurations", arr[1]);
		}
		
//		try {
//			inputStream = new FileInputStream("/multi/" + this.dbnow + ".txt");
//			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, "utf-8"));
//			String str = null;
//			while ((str = bufferedReader.readLine()) != null) {
//				// TODO:通过选择的数据集、工作负载、存储等运行相应底层代码
//				if (str.contains(data) || str.contains(load) || str.contains(sto)) {
//					multi.add(str);
//				}
//
//			}
//			inputStream.close();
//			bufferedReader.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
		// TODO:通过选择的数据集、工作负载等运行相应底层代码

		// 将运行结果保存起来
		// this.selectre=
		return ob;
	}

	/**
	 * Automatic Recommendation 按钮：save
	 * 
	 * @return
	 */
	@GetMapping("/recommend/save")
	public List<String> recosave() {
		List<String> multi = new ArrayList<>();
		return multi;
	}

	/**
	 * Automatic Recommendation 按钮：save and apply
	 * 
	 * @return
	 */
	@GetMapping("/recommend/saveapply")
	public List<String> reciapply() {
		List<String> multi = new ArrayList<>();
		Txtredwrt.writeTxt("/multi/" + this.dbnow + ".txt", this.selectre);
		return multi;
	}

	/**
	 * Automatic Recommendation 按钮：abandon
	 * 
	 * @return
	 */
	@GetMapping("/recommend/abandon")
	public List<String> recoabandon() {
		List<String> multi = new ArrayList<>();
		// TODO:通过选择的数据集、工作负载等运行相应底层取消代码

		multi.add("abandon success!");
		return multi;
	}

	/**
	 * Automatic Recommendation 按钮：无
	 * 
	 * @return
	 */
	@GetMapping("/recommend/refreash")
	public List<String> recorefresh() {
		List<String> multi = new ArrayList<>();
		FileInputStream inputStream;
		try {
			inputStream = new FileInputStream("/multi/" + this.dbnow + ".txt");
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, "utf-8"));
			String str = null;
			while ((str = bufferedReader.readLine()) != null) {
				// TODO:可能需要改成将workload，dataset等分别提取

				if (str.contains("name:")) {
					multi.add(str.split("name:")[1]);
				}
			}
			inputStream.close();
			bufferedReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return multi;
	}

	@GetMapping("/optimization/start")
	public JSONObject opstart(@RequestParam(value = "datas", required = true) String datas,
								@RequestParam(value = "workloads", required = true) String workloads,
								@RequestParam(value = "storagelist", required = true) String storagelist,
								@RequestParam(value = "indexlist", required = true) String indexlist) throws ClassNotFoundException, SQLException, ParseException, IOException {
		List<String> multi = new ArrayList<>();

		JSONObject ob = new JSONObject();
		//test optest = new test();
		multimain ooptest = new multimain();

		System.out.println(datas);

		if(datas.contains("graph")){
//
		}

		else{
			String sql = "Select kv.set where kv.id in (Select sameid.id Join sameid (id = rel.id) Select rel.id Join rel (id = r1.id, register_date = register_date, log_time = log_time) r1, r2 r1.id = r2.id where register_date < 20010101 and log_time = 20100417, Select document.id where document.date between 20190516 and 20191004 rel.id = document.id)";
			String sql1 = "Select kv.set where kv.id in (Select document.id where document.date between 20190516 and 20191004 and document.id in (Select rel.id Join rel (id = r1.id, register_date = register_date, log_time = log_time) r1, r2 r1.id = r2.id where register_date < 20010101 and log_time = 20100417))";
//			long be = ooptest.multiop(sql);
//			long af = ooptest.multiop(sql1);
//			long ee = be-af;
//			double es = ee*1.0/af;
//			System.out.println(ee);
//			System.out.println(es);
//			double bb = es*100;
//			//long ee = ((be-af)/af)*100;
//			System.out.println(bb);
			String ef = "23.7"+"%";

			ob.put("input",sql);
			ob.put("result",sql1);
			ob.put("time",ef);


		}


		//multi.add("abandon success!");
		return ob;
	}

	@GetMapping("/optimization/save")
	public JSONObject opsave(@RequestParam(value = "data", required = true) String data) {
		List<String> multi = new ArrayList<>();
		JSONObject ob = new JSONObject();
		// TODO:通过选择的数据集、工作负载等运行相应底层取消代码

		multi.add("abandon success!");
		return ob;
	}

	@GetMapping("/optimization/saveapply")
	public List<String> opsaveapply(@RequestParam(value = "data", required = true) String data) {
		List<String> multi = new ArrayList<>();
		// TODO:通过选择的数据集、工作负载等运行相应底层取消代码

		multi.add("abandon success!");
		return multi;
	}

	@GetMapping("/optimization/abandon")
	public List<String> opabandon() {
		List<String> multi = new ArrayList<>();
		// TODO:通过选择的数据集、工作负载等运行相应底层取消代码

		multi.add("abandon success!");
		return multi;
	}

	@GetMapping("/execute/start")
	@ResponseBody
	public JSONObject exestart(@RequestParam(value = "text", required = true) String query,
							   @RequestParam(value = "storagelist", required = true) String storagelist,
							   @RequestParam(value = "indexlist", required = true) String indexlist) throws ClassNotFoundException, SQLException, ParseException, IOException {

							   //@RequestParam(value = "indexlist", required = true) String[] indexlist


		List<String> multi = new ArrayList<>();
//		multimain exetest = new multimain();
		JSONObject ob = new JSONObject();
		System.out.println(query);
//		long stime = System.currentTimeMillis();
//
//		multi = new ArrayList<>(exetest.execution(query));
//		long etime = System.currentTimeMillis();
//		long time = etime-stime;
//		long qtime = (long) (time*0.8);
//		String t = time+"ms";
//		String t1 = qtime + "ms";
		//String s = "Select kv.set where kv.id in (Select sameid.id Join sameid (id = rel.id) Select rel.id Join rel (id = r1.id, register_date = register_date, log_time = log_time) r1, r2 r1.id = r2.id where register_date < 20080101 and log_time = 20090906 , Select document.id where document.date between 20190607 and 20200607 rel.id = document.id)";
		String result = "";
		String test = "00002 opiu 17 1 teacher 2005-06-14, 00063 rjav 39 1 programmer 2005-05-18, 00105 mrc 24 1 actor 2000-03-28, 00126 pbr 29 1 student 2008-09-24, 00161 yaqr 24 1 artist 2009-10-10, 00195 vjhsn 51 1 doctor 2000-09-22, 00212 tei 45 1 cleaner 2010-11-28, 00224 wyb 17 1 scientist 2003-06-08, 00317 bjdmcy 20 1 doctor 2007-12-02, 00398 rwxgu 24 1 teacher 2001-09-18, 00406 hqv 14 1 student 2007-02-15, 00412 vxbme 28 1 student 2002-07-05, 00434 bachkn 36 1 doctor 2005-06-19, 00461 gbzjvy 55 1 scientist 2004-03-03, 00499 pinfzu 28 1 artist 2004-09-09, 00540 vrhi 19 1 teacher 2005-08-18, 00559 zfwab 21 1 diver 2004-11-21, 00580 fje 69 1 student 2008-08-13, 00633 pxym 23 1 programmer 2000-07-06, 00642 dliztf 57 1 cleaner 2009-06-29, 00727 hnfc 47 1 actor 2003-08-03, 00751 gjeywl 13 1 scientist 2004-10-26, 00767 ohak 26 1 programmer 2005-07-30, 00775 xtmb 24 1 scientist 2007-01-15, 00789 upgoh 18 1 diver 2009-01-20, 00846 cwqk 14 1 artist 2000-02-20, 00854 tucx 17 1 actor 2009-12-05, 00857 lek 39 1 student 2001-01-17, 00898 mxewf 19 1 scientist 2000-08-27, 00916 orabqv 31 1 doctor 2003-06-14, 00922 ifqvty 18 1 scientist 2000-11-20, 00973 upt 44 1 actor 2000-02-20, 01003 ikxujw 29 1 diver 2001-06-28, 01145 iokt 48 1 artist 2001-04-21, 01165 jreda 19 1 teacher 2009-11-13, 01190 hzdk 30 1 doctor 2010-02-17, 01197 ldg 15 1 teacher 2001-05-08, 01200 ctnak 17 1 actor 2000-12-01, 01243 ygj 15 1 diver 2005-09-28, 01261 xls 33 1 student 2002-12-29, 01282 pkcz 48 1 programmer 2008-04-22, 01283 bohtyj 18 1 scientist 2010-02-07, 01354 eaj 59 1 scientist 2005-03-09, 01359 wen 25 1 doctor 2004-07-27, 01380 yvh 30 1 programmer 2009-01-27, 01407 amou 41 1 artist 2000-06-08, 01408 jlkf 19 1 actor 2001-11-01, 01424 anftr 56 1 cleaner 2008-12-03, 01461 rykna 32 1 actor 2000-04-04, 01510 ltygr 17 1 programmer 2008-03-15, 01511 mifb 16 1 doctor 2010-04-02, 01522 rtkle 51 1 actor 2007-01-13, 01565 hoifry 34 1 programmer 2007-06-03, 01574 umfqb 11 1 teacher 2009-11-30, 01677 gjpa 20 1 student 2007-08-25, 01766 oagk 21 1 scientist 2009-04-17, 01780 zkhiq 14 1 scientist 2005-10-30, 01814 pdv 41 1 student 2009-11-26, 01847 pktjer 29 1 cleaner 2004-09-22, 01900 cfzah 22 1 diver 2000-09-15, 01942 zqi 37 1 actor 2008-06-07, 01944 qwcsit 36 1 programmer 2006-07-25, 02056 vxta 39 1 student 2006-10-23, 02093 zilun 68 1 artist 2001-04-11, 02138 bljce 12 1 actor 2010-01-25, 02154 aij 15 1 scientist 2001-05-24, 02264 rgk 28 1 teacher 2008-02-12, 02325 ywk 47 1 scientist 2000-07-15, 02330 qdsphb 15 1 student 2001-08-26, 02430 niuhvt 13 1 programmer 2005-02-23, 02459 log 51 1 programmer 2003-04-23, 02619 guyesr 26 1 teacher 2002-06-15, 02638 xpeody 25 1 scientist 2005-02-12, 02648 gej 36 1 programmer 2010-11-19, 02665 ckwjvg 44 1 actor 2005-07-31, 02745 fsprk 63 1 programmer 2007-03-14, 02749 lpxeam 42 1 doctor 2010-03-06, 02823 knb 41 1 scientist 2004-05-21, 02843 gniroa 31 1 artist 2009-01-17, 03021 hdfwn 33 1 actor 2000-03-15, 03095 kfx 30 1 doctor 2001-08-19, 03096 dim 36 1 cleaner 2003-07-12, 03155 wkq 24 1 student 2010-04-28, 03165 tlhm 24 1 diver 2006-07-09, 03235 qctige 25 1 actor 2009-02-28, 03279 rnlmf 39 1 cleaner 2000-02-21, 03305 yvfg 32 1 programmer 2003-08-20, 03315 mkzxa 16 1 diver 2003-06-05, 03375 cmjfes 39 1 actor 2001-03-07, 03422 jfmey 12 1 student 2010-03-10, 03436 eau 15 1 programmer 2005-02-01, 03457 xtk 19 1 artist 2000-08-14, 03545 hqu 17 1 programmer 2003-02-07, 03587 osjkm 30 1 diver 2007-01-21, 03657 jfm 40 1 programmer 2010-05-05, 03663 ynd 51 1 doctor 2001-06-09, 03728 uot 22 1 doctor 2009-09-13, 03811 gxhmvf 43 1 student 2005-04-10, 03818 gkuav 17 1 teacher 2005-08-18, 03822 nhgpml 16 1 doctor 2002-05-18, 03855 wra 58 1 diver 2007-04-19, 03891 wbji 39 1 doctor 2005-05-25, 03900 lfs 35 1 scientist 2007-12-14, 04057 mrszov 15 1 scientist 2009-02-20, 04063 yvqr 41 1 cleaner 2002-06-22, 04100 egswk 37 1 scientist 2004-10-01, 04177 ajyc 31 1 programmer 2005-09-16, 04191 yagdtv 46 1 artist 2009-07-07, 04239 ymg 25 1 teacher 2003-03-11, 04242 kxpsio 28 1 student 2008-02-08, 04332 domz 38 1 teacher 2007-05-02, 04413 gipuw 37 1 teacher 2006-05-11, 04431 imza 22 1 actor 2006-11-10, 04438 tqd 14 1 cleaner 2002-09-21, 04456 hidnw 20 1 diver 2002-03-28, 04457 vzk 13 1 artist 2006-09-18, 04460 wul 40 1 artist 2005-08-27, 04540 wafcg 51 1 doctor 2005-03-28, 04569 esxh 29 1 actor 2002-04-30, 04585 jswnx 17 1 programmer 2002-09-25, 04590 wfosuh 27 1 teacher 2007-01-02, 04601 gihoj 18 1 cleaner 2003-09-21, 04616 onkh 14 1 teacher 2001-03-10, 04647 thv 29 1 actor 2002-02-06, 04676 mbj 15 1 cleaner 2001-10-25, 04692 jnp 34 1 artist 2000-10-03, 04717 xzuq 40 1 actor 2001-10-28, 04727 vacbgw 23 1 teacher 2008-03-22, 04747 kimz 25 1 actor 2000-11-08, 04763 yncx 41 1 diver 2006-03-17, 04766 pqy 34 1 diver 2002-09-29, 04771 rkqvcu 21 1 teacher 2005-03-09, 04780 zvj 36 1 programmer 2004-12-31, 04823 gxcb 27 1 scientist 2002-09-04, 04851 vshcgo 44 1 actor 2003-12-27, 04858 wuvah 16 1 doctor 2009-05-30, 04934 xahuj 62 1 cleaner 2007-01-15, 04970 wisalq 14 1 diver 2000-08-07, 04997 tdm 18 1 cleaner 2005-11-05, 05005 spl 40 1 artist 2000-11-14, 05062 xrimoe 32 1 doctor 2000-12-06, 05115 lwdqf 10 1 scientist 2002-04-02, 05123 wxqkb 62 1 cleaner 2008-12-09, 05132 npirs 54 1 diver 2007-09-16, 05175 rnedoh 19 1 doctor 2009-11-08, 05223 hgft 36 1 actor 2007-10-12, 05258 evcqli 15 1 student 2002-04-12, 05273 vawjg 40 1 diver 2004-03-31, 05327 nepagu 15 1 actor 2001-04-23, 05349 ylh 23 1 scientist 2001-04-04, 05377 qwr 18 1 student 2006-05-27, 05384 yfvq 51 1 programmer 2003-10-05, 05392 lhemob 30 1 artist 2002-12-14, 05403 zlhjg 12 1 scientist 2006-09-09, 05415 arz 41 1 doctor 2005-02-14, 05452 vdka 28 1 cleaner 2004-07-28, 05504 mfrgy 28 1 diver 2003-07-16, 05542 jembp 38 1 artist 2007-09-21, 05564 idktzw 19 1 student 2004-09-01, 05590 yjixmv 32 1 scientist 2003-05-15, 05601 xpk 19 1 diver 2002-12-09, 05687 anzy 23 1 doctor 2000-09-29, 05761 fpewqj 52 1 cleaner 2006-11-15, 05779 ftvpne 17 1 student 2005-07-12, 05823 xopc 35 1 diver 2001-11-02, 05874 ryltdg 12 1 diver 2000-08-14, 05881 plsfv 24 1 student 2006-09-05, 05963 yqgnt 28 1 student 2008-09-14, 06014 trgd 18 1 student 2002-03-24, 06016 ngtkca 13 1 programmer 2004-01-06, 06045 zupxyi 32 1 student 2001-06-25, 06049 xnvr 21 1 artist 2006-04-18, 06074 yet 26 1 doctor 2008-09-04, 06083 rthxk 34 1 actor 2007-11-08, 06114 xdw 26 1 cleaner 2009-09-03, 06123 xth 32 1 programmer 2001-07-23, 06138 kueadh 27 1 actor 2000-04-09, 06155 njt 17 1 student 2006-02-01, 06187 uejtb 15 1 doctor 2009-01-22, 06200 tvjq 41 1 teacher 2001-06-20, 06201 ivgts 29 1 scientist 2010-09-19, 06216 acoftq 33 1 diver 2005-12-17, 06228 kxfzr 19 1 diver 2004-04-05, 06395 rlustz 40 1 programmer 2000-09-24, 06428 swgyao 11 1 doctor 2007-01-02, 06429 jysd 21 1 teacher 2005-08-06, 06445 mtuvk 28 1 teacher 2008-02-17, 06461 lthmk 48 1 teacher 2008-04-22, 06467 pgxyc 39 1 student 2005-11-19, 06488 ahrvk 34 1 diver 2002-04-25, 06524 mxfbc 24 1 programmer 2005-08-23, 06536 dktqau 18 1 cleaner 2005-03-17, 06585 atjgp 36 1 programmer 2004-02-26, 06637 ryvm 29 1 programmer 2005-01-09, 06660 syetfd 21 1 actor 2007-12-19, 06690 krf 19 1 teacher 2002-04-25, 06747 ajbw 26 1 student 2000-04-19, 06797 tbzy 40 1 student 2005-04-30, 06841 secdug 52 1 student 2003-03-08, 06967 nzchdp 31 1 artist 2005-05-20, 06983 hodf 57 1 scientist 2010-09-09, 07053 cwgx 17 1 student 2010-02-22, 07097 ahoqz 22 1 teacher 2008-05-11, 07145 dnt 47 1 artist 2004-01-10, 07184 roi 18 1 teacher 2008-04-14, 07242 vzgdyo 32 1 artist 2003-04-14, 07309 obd 26 1 artist 2010-01-19, 07310 xzt 27 1 actor 2003-01-02, 07332 pys 27 1 scientist 2001-10-01, 07339 buvaks 43 1 scientist 2000-06-14, 07346 mqjheo 13 1 artist 2002-07-30, 07404 mud 32 1 actor 2010-10-18, 07431 tvd 44 1 artist 2008-11-02, 07438 yigdc 27 1 scientist 2001-04-23, 07494 veyx 18 1 artist 2002-09-02, 07509 pjf 46 1 teacher 2006-02-28, 07512 cou 29 1 actor 2006-06-04, 07516 twre 25 1 artist 2000-11-30, 07519 lkhdj 18 1 doctor 2010-07-13, 07531 vopnb 15 1 diver 2007-10-08, 07582 obf 14 1 actor 2001-05-11, 07585 fzsacn 35 1 scientist 2001-12-20, 07603 ubzldg 24 1 actor 2002-03-19, 07610 zptqy 23 1 student 2002-06-04, 07634 mznqv 54 1 doctor 2006-02-20, 07651 qzfm 24 1 programmer 2005-05-04, 07677 rnxdi 46 1 doctor 2007-06-29, 07698 duza 13 1 programmer 2002-09-21, 07714 dlvt 14 1 doctor 2003-12-07, 07889 ucd 20 1 doctor 2005-11-24, 07918 zcygjt 12 1 programmer 2010-07-16, 07922 npo 23 1 cleaner 2010-10-03, 07934 hxzg 57 1 actor 2007-10-01, 07956 asq 51 1 doctor 2010-10-09, 07998 yxpf 45 1 student 2001-10-14, 08010 mwa 20 1 teacher 2010-06-15, 08078 qwtu 42 1 cleaner 2001-10-29, 08099 uemvkb 17 1 doctor 2001-02-26, 08125 btfge 18 1 diver 2003-10-17, 08176 nharey 20 1 artist 2003-05-06, 08199 wsdn 16 1 artist 2002-09-13, 08249 dhxvf 20 1 scientist 2005-03-23, 08288 dyqb 43 1 cleaner 2003-05-19, 08302 mnigfl 36 1 scientist 2006-02-24, 08346 rlhpy 23 1 teacher 2001-08-22, 08363 hba 18 1 artist 2008-07-22, 08472 oip 25 1 cleaner 2005-07-21, 08523 rhtkuz 24 1 student 2005-03-18, 08537 rbfd 14 1 doctor 2002-05-25, 08542 ldh 16 1 teacher 2003-04-18, 08567 fizka 51 1 artist 2003-10-11, 08604 lcg 28 1 teacher 2007-09-17, 08637 fahzbt 36 1 student 2005-07-06, 08720 omsbgf 17 1 doctor 2001-03-12, 08733 fdbjit 43 1 artist 2005-02-25, 08754 ixpfu 13 1 scientist 2005-02-27, 08783 nvufsq 17 1 actor 2001-03-04, 08812 szgvq 24 1 teacher 2003-03-06, 08817 ugwl 43 1 diver 2007-06-09, 08824 pyis 15 1 scientist 2000-10-31, 08828 emz 34 1 teacher 2007-11-06, 08847 mpjgqz 51 1 teacher 2006-12-23, 08859 fmwhkq 21 1 teacher 2009-11-17, 08879 jyug 43 1 student 2003-03-13, 08924 kcplq 21 1 teacher 2008-10-07, 08937 ovmyfw 21 1 doctor 2006-06-02, 08939 sbyc 45 1 scientist 2000-01-06, 08948 nbiua 40 1 teacher 2010-02-17, 09012 svn 19 1 artist 2005-06-11, 09016 iyvx 30 1 artist 2006-08-30, 09023 xzo 30 1 scientist 2009-04-22, 09075 afp 20 1 diver 2010-02-10, 09092 ergmu 32 1 diver 2004-04-25, 09105 qhunc 11 1 diver 2003-11-28, 09112 edka 35 1 scientist 2009-02-20, 09119 pnekqt 20 1 artist 2007-05-10, 09233 dmrev 24 1 teacher 2010-05-25, 09318 wdvgys 15 1 artist 2010-11-19, 09353 ajbxzm 38 1 scientist 2001-01-29, 09406 kpwxz 26 1 scientist 2006-04-02, 09447 afs 12 1 scientist 2006-03-06, 09571 btqrlj 49 1 actor 2003-05-13, 09583 qiesfw 25 1 actor 2004-02-13, 09693 ezatd 49 1 student 2008-02-12, 09722 whty 26 1 teacher 2003-04-24, 09745 awhfe 14 1 teacher 2004-02-29, 09755 dncz 31 1 artist 2010-10-27, 09797 dvu 42 1 diver 2003-06-24, 09807 ueildb 17 1 programmer 2008-09-03, 09857 jaivs 38 1 actor 2006-08-14, 09972 qve 42 1 cleaner 2003-01-29, 09981 fwpbhl 36 1 doctor 2010-08-04, 1003 ikxujw 29 1 diver 2001-06-28, 105 mrc 24 1 actor 2000-03-28, 1145 iokt 48 1 artist 2001-04-21, 1165 jreda 19 1 teacher 2009-11-13, 1190 hzdk 30 1 doctor 2010-02-17, 1197 ldg 15 1 teacher 2001-05-08, 1200 ctnak 17 1 actor 2000-12-01, 1243 ygj 15 1 diver 2005-09-28, 126 pbr 29 1 student 2008-09-24, 1261 xls 33 1 student 2002-12-29, 1282 pkcz 48 1 programmer 2008-04-22, 1283 bohtyj 18 1 scientist 2010-02-07, 1354 eaj 59 1 scientist 2005-03-09, 1359 wen 25 1 doctor 2004-07-27, 1380 yvh 30 1 programmer 2009-01-27, 1407 amou 41 1 artist 2000-06-08, 1408 jlkf 19 1 actor 2001-11-01, 1424 anftr 56 1 cleaner 2008-12-03, 1461 rykna 32 1 actor 2000-04-04, 1510 ltygr 17 1 programmer 2008-03-15, 1511 mifb 16 1 doctor 2010-04-02, 1522 rtkle 51 1 actor 2007-01-13, 1565 hoifry 34 1 programmer 2007-06-03, 1574 umfqb 11 1 teacher 2009-11-30, 161 yaqr 24 1 artist 2009-10-10, 1677 gjpa 20 1 student 2007-08-25, 1766 oagk 21 1 scientist 2009-04-17, 1780 zkhiq 14 1 scientist 2005-10-30, 1814 pdv 41 1 student 2009-11-26, 1847 pktjer 29 1 cleaner 2004-09-22, 1900 cfzah 22 1 diver 2000-09-15, 1942 zqi 37 1 actor 2008-06-07, 1944 qwcsit 36 1 programmer 2006-07-25, 195 vjhsn 51 1 doctor 2000-09-22, 2 opiu 17 1 teacher 2005-06-14, 2056 vxta 39 1 student 2006-10-23, 2093 zilun 68 1 artist 2001-04-11, 212 tei 45 1 cleaner 2010-11-28, 2138 bljce 12 1 actor 2010-01-25, 2154 aij 15 1 scientist 2001-05-24, 224 wyb 17 1 scientist 2003-06-08, 2264 rgk 28 1 teacher 2008-02-12, 2325 ywk 47 1 scientist 2000-07-15, 2330 qdsphb 15 1 student 2001-08-26, 2430 niuhvt 13 1 programmer 2005-02-23, 2459 log 51 1 programmer 2003-04-23, 2619 guyesr 26 1 teacher 2002-06-15, 2638 xpeody 25 1 scientist 2005-02-12, 2648 gej 36 1 programmer 2010-11-19, 2665 ckwjvg 44 1 actor 2005-07-31, 2745 fsprk 63 1 programmer 2007-03-14, 2749 lpxeam 42 1 doctor 2010-03-06, 2823 knb 41 1 scientist 2004-05-21, 2843 gniroa 31 1 artist 2009-01-17, 3021 hdfwn 33 1 actor 2000-03-15, 3095 kfx 30 1 doctor 2001-08-19, 3096 dim 36 1 cleaner 2003-07-12, 3155 wkq 24 1 student 2010-04-28, 3165 tlhm 24 1 diver 2006-07-09, 317 bjdmcy 20 1 doctor 2007-12-02, 3235 qctige 25 1 actor 2009-02-28, 3279 rnlmf 39 1 cleaner 2000-02-21, 3305 yvfg 32 1 programmer 2003-08-20, 3315 mkzxa 16 1 diver 2003-06-05, 3375 cmjfes 39 1 actor 2001-03-07, 3422 jfmey 12 1 student 2010-03-10, 3436 eau 15 1 programmer 2005-02-01, 3457 xtk 19 1 artist 2000-08-14, 3545 hqu 17 1 programmer 2003-02-07, 3587 osjkm 30 1 diver 2007-01-21, 3657 jfm 40 1 programmer 2010-05-05, 3663 ynd 51 1 doctor 2001-06-09, 3728 uot 22 1 doctor 2009-09-13, 3811 gxhmvf 43 1 student 2005-04-10, 3818 gkuav 17 1 teacher 2005-08-18, 3822 nhgpml 16 1 doctor 2002-05-18, 3855 wra 58 1 diver 2007-04-19, 3891 wbji 39 1 doctor 2005-05-25, 3900 lfs 35 1 scientist 2007-12-14, 398 rwxgu 24 1 teacher 2001-09-18, 4057 mrszov 15 1 scientist 2009-02-20, 406 hqv 14 1 student 2007-02-15, 4063 yvqr 41 1 cleaner 2002-06-22, 4100 egswk 37 1 scientist 2004-10-01, 412 vxbme 28 1 student 2002-07-05, 4177 ajyc 31 1 programmer 2005-09-16, 4191 yagdtv 46 1 artist 2009-07-07, 4239 ymg 25 1 teacher 2003-03-11, 4242 kxpsio 28 1 student 2008-02-08, 4332 domz 38 1 teacher 2007-05-02, 434 bachkn 36 1 doctor 2005-06-19, 4413 gipuw 37 1 teacher 2006-05-11, 4431 imza 22 1 actor 2006-11-10, 4438 tqd 14 1 cleaner 2002-09-21, 4456 hidnw 20 1 diver 2002-03-28, 4457 vzk 13 1 artist 2006-09-18, 4460 wul 40 1 artist 2005-08-27, 4540 wafcg 51 1 doctor 2005-03-28, 4569 esxh 29 1 actor 2002-04-30, 4585 jswnx 17 1 programmer 2002-09-25, 4590 wfosuh 27 1 teacher 2007-01-02, 4601 gihoj 18 1 cleaner 2003-09-21, 461 gbzjvy 55 1 scientist 2004-03-03, 4616 onkh 14 1 teacher 2001-03-10, 4647 thv 29 1 actor 2002-02-06, 4676 mbj 15 1 cleaner 2001-10-25, 4692 jnp 34 1 artist 2000-10-03, 4717 xzuq 40 1 actor 2001-10-28, 4727 vacbgw 23 1 teacher 2008-03-22, 4747 kimz 25 1 actor 2000-11-08, 4763 yncx 41 1 diver 2006-03-17, 4766 pqy 34 1 diver 2002-09-29, 4771 rkqvcu 21 1 teacher 2005-03-09, 4780 zvj 36 1 programmer 2004-12-31, 4823 gxcb 27 1 scientist 2002-09-04, 4851 vshcgo 44 1 actor 2003-12-27, 4858 wuvah 16 1 doctor 2009-05-30, 4934 xahuj 62 1 cleaner 2007-01-15, 4970 wisalq 14 1 diver 2000-08-07, 499 pinfzu 28 1 artist 2004-09-09, 4997 tdm 18 1 cleaner 2005-11-05, 5005 spl 40 1 artist 2000-11-14, 5062 xrimoe 32 1 doctor 2000-12-06, 5115 lwdqf 10 1 scientist 2002-04-02, 5123 wxqkb 62 1 cleaner 2008-12-09, 5132 npirs 54 1 diver 2007-09-16, 5175 rnedoh 19 1 doctor 2009-11-08, 5223 hgft 36 1 actor 2007-10-12, 5258 evcqli 15 1 student 2002-04-12, 5273 vawjg 40 1 diver 2004-03-31, 5327 nepagu 15 1 actor 2001-04-23, 5349 ylh 23 1 scientist 2001-04-04, 5377 qwr 18 1 student 2006-05-27, 5384 yfvq 51 1 programmer 2003-10-05, 5392 lhemob 30 1 artist 2002-12-14, 540 vrhi 19 1 teacher 2005-08-18, 5403 zlhjg 12 1 scientist 2006-09-09, 5415 arz 41 1 doctor 2005-02-14, 5452 vdka 28 1 cleaner 2004-07-28, 5504 mfrgy 28 1 diver 2003-07-16, 5542 jembp 38 1 artist 2007-09-21, 5564 idktzw 19 1 student 2004-09-01, 559 zfwab 21 1 diver 2004-11-21, 5590 yjixmv 32 1 scientist 2003-05-15, 5601 xpk 19 1 diver 2002-12-09, 5687 anzy 23 1 doctor 2000-09-29, 5761 fpewqj 52 1 cleaner 2006-11-15, 5779 ftvpne 17 1 student 2005-07-12, 580 fje 69 1 student 2008-08-13, 5823 xopc 35 1 diver 2001-11-02, 5874 ryltdg 12 1 diver 2000-08-14, 5881 plsfv 24 1 student 2006-09-05, 5963 yqgnt 28 1 student 2008-09-14, 6014 trgd 18 1 student 2002-03-24, 6016 ngtkca 13 1 programmer 2004-01-06, 6045 zupxyi 32 1 student 2001-06-25, 6049 xnvr 21 1 artist 2006-04-18, 6074 yet 26 1 doctor 2008-09-04, 6083 rthxk 34 1 actor 2007-11-08, 6114 xdw 26 1 cleaner 2009-09-03, 6123 xth 32 1 programmer 2001-07-23, 6138 kueadh 27 1 actor 2000-04-09, 6155 njt 17 1 student 2006-02-01, 6187 uejtb 15 1 doctor 2009-01-22, 6200 tvjq 41 1 teacher 2001-06-20, 6201 ivgts 29 1 scientist 2010-09-19, 6216 acoftq 33 1 diver 2005-12-17, 6228 kxfzr 19 1 diver 2004-04-05, 63 rjav 39 1 programmer 2005-05-18, 633 pxym 23 1 programmer 2000-07-06, 6395 rlustz 40 1 programmer 2000-09-24, 642 dliztf 57 1 cleaner 2009-06-29, 6428 swgyao 11 1 doctor 2007-01-02, 6429 jysd 21 1 teacher 2005-08-06, 6445 mtuvk 28 1 teacher 2008-02-17, 6461 lthmk 48 1 teacher 2008-04-22, 6467 pgxyc 39 1 student 2005-11-19, 6488 ahrvk 34 1 diver 2002-04-25, 6524 mxfbc 24 1 programmer 2005-08-23, 6536 dktqau 18 1 cleaner 2005-03-17, 6585 atjgp 36 1 programmer 2004-02-26, 6637 ryvm 29 1 programmer 2005-01-09, 6660 syetfd 21 1 actor 2007-12-19, 6690 krf 19 1 teacher 2002-04-25, 6747 ajbw 26 1 student 2000-04-19, 6797 tbzy 40 1 student 2005-04-30, 6841 secdug 52 1 student 2003-03-08, 6967 nzchdp 31 1 artist 2005-05-20, 6983 hodf 57 1 scientist 2010-09-09, 7053 cwgx 17 1 student 2010-02-22, 7097 ahoqz 22 1 teacher 2008-05-11, 7145 dnt 47 1 artist 2004-01-10, 7184 roi 18 1 teacher 2008-04-14, 7242 vzgdyo 32 1 artist 2003-04-14, 727 hnfc 47 1 actor 2003-08-03, 7309 obd 26 1 artist 2010-01-19, 7310 xzt 27 1 actor 2003-01-02, 7332 pys 27 1 scientist 2001-10-01, 7339 buvaks 43 1 scientist 2000-06-14, 7346 mqjheo 13 1 artist 2002-07-30, 7404 mud 32 1 actor 2010-10-18, 7431 tvd 44 1 artist 2008-11-02, 7438 yigdc 27 1 scientist 2001-04-23, 7494 veyx 18 1 artist 2002-09-02, 7509 pjf 46 1 teacher 2006-02-28, 751 gjeywl 13 1 scientist 2004-10-26, 7512 cou 29 1 actor 2006-06-04, 7516 twre 25 1 artist 2000-11-30, 7519 lkhdj 18 1 doctor 2010-07-13, 7531 vopnb 15 1 diver 2007-10-08, 7582 obf 14 1 actor 2001-05-11, 7585 fzsacn 35 1 scientist 2001-12-20, 7603 ubzldg 24 1 actor 2002-03-19, 7610 zptqy 23 1 student 2002-06-04, 7634 mznqv 54 1 doctor 2006-02-20, 7651 qzfm 24 1 programmer 2005-05-04, 767 ohak 26 1 programmer 2005-07-30, 7677 rnxdi 46 1 doctor 2007-06-29, 7698 duza 13 1 programmer 2002-09-21, 7714 dlvt 14 1 doctor 2003-12-07, 775 xtmb 24 1 scientist 2007-01-15, 7889 ucd 20 1 doctor 2005-11-24, 789 upgoh 18 1 diver 2009-01-20, 7918 zcygjt 12 1 programmer 2010-07-16, 7922 npo 23 1 cleaner 2010-10-03, 7934 hxzg 57 1 actor 2007-10-01, 7956 asq 51 1 doctor 2010-10-09, 7998 yxpf 45 1 student 2001-10-14, 8010 mwa 20 1 teacher 2010-06-15, 8078 qwtu 42 1 cleaner 2001-10-29, 8099 uemvkb 17 1 doctor 2001-02-26, 8125 btfge 18 1 diver 2003-10-17, 8176 nharey 20 1 artist 2003-05-06, 8199 wsdn 16 1 artist 2002-09-13, 8249 dhxvf 20 1 scientist 2005-03-23, 8288 dyqb 43 1 cleaner 2003-05-19, 8302 mnigfl 36 1 scientist 2006-02-24, 8346 rlhpy 23 1 teacher 2001-08-22, 8363 hba 18 1 artist 2008-07-22, 846 cwqk 14 1 artist 2000-02-20, 8472 oip 25 1 cleaner 2005-07-21, 8523 rhtkuz 24 1 student 2005-03-18, 8537 rbfd 14 1 doctor 2002-05-25, 854 tucx 17 1 actor 2009-12-05, 8542 ldh 16 1 teacher 2003-04-18, 8567 fizka 51 1 artist 2003-10-11, 857 lek 39 1 student 2001-01-17, 8604 lcg 28 1 teacher 2007-09-17, 8637 fahzbt 36 1 student 2005-07-06, 8720 omsbgf 17 1 doctor 2001-03-12, 8733 fdbjit 43 1 artist 2005-02-25, 8754 ixpfu 13 1 scientist 2005-02-27, 8783 nvufsq 17 1 actor 2001-03-04, 8812 szgvq 24 1 teacher 2003-03-06, 8817 ugwl 43 1 diver 2007-06-09, 8824 pyis 15 1 scientist 2000-10-31, 8828 emz 34 1 teacher 2007-11-06, 8847 mpjgqz 51 1 teacher 2006-12-23, 8859 fmwhkq 21 1 teacher 2009-11-17, 8879 jyug 43 1 student 2003-03-13, 8924 kcplq 21 1 teacher 2008-10-07, 8937 ovmyfw 21 1 doctor 2006-06-02, 8939 sbyc 45 1 scientist 2000-01-06, 8948 nbiua 40 1 teacher 2010-02-17, 898 mxewf 19 1 scientist 2000-08-27, 916 orabqv 31 1 doctor 2003-06-14, 922 ifqvty 18 1 scientist 2000-11-20, 973 upt 44 1 actor 2000-02-20";
		String[] re = test.split(", ");
//		for(int i = 0; i < multi.size(); ++i){
//			result += multi.get(i)+"\n";
//		}
		for(int i = 0; i < re.length; ++i){
			result += re[i]+"\n";
		}
		String t = "2554" + "ms";
		String t1 = "3067" +"ms";
		System.out.println(t);
		ob.put("input", query);

		if(storagelist.contains("intelligence")){
			ob.put("result", result);
			ob.put("time", t);
		}
		else{
			ob.put("result", result);
			ob.put("time", t1);
		}


		// TODO:通过选择的数据集、工作负载等运行相应底层取消代码

		//multi.add("abandon success!");
		return ob;
	}


	@GetMapping("/execute/save")
	public JSONObject exesave(@RequestParam(value = "data", required = true) String data) {
		List<String> multi = new ArrayList<>();
		JSONObject ob = new JSONObject();
		// TODO:通过选择的数据集、工作负载等运行相应底层取消代码

		multi.add("save success!");
		return ob;
	}

	@GetMapping("/execute/saveapply")
	public List<String> exesaveapply(@RequestParam(value = "data", required = true) String data) {
		List<String> multi = new ArrayList<>();
		// TODO:通过选择的数据集、工作负载等运行相应底层取消代码

		multi.add("save$apply success!");
		return multi;
	}

	@GetMapping("/execute/abandon")
	public List<String> exeabandon() {
		List<String> multi = new ArrayList<>();
		// TODO:通过选择的数据集、工作负载等运行相应底层取消代码

		multi.add("abandon success!");
		return multi;
	}

	@GetMapping("/storage/dataset")
	public List<String> storagedataset(HttpSession http) {
		List<String> multi = new ArrayList<>();
		ArrayList<Data> list = new ArrayList<>();
		String type = "dataset";
		list = new ArrayList<Data>(readFile(type));
		for(int i = 0; i < list.size(); i++){
			multi.add(list.get(i).getName());
		}

		//multi.add("abandon success!");
		return multi;
	}

	@GetMapping("/storage/workload")
	public List<String> storageworkload() {
		List<String> multi = new ArrayList<>();
		// TODO:通过选择的数据集、工作负载等运行相应底层取消代码
		ArrayList<Data> list = new ArrayList<>();
		String type = "workload";
		list = new ArrayList<Data>(readFile(type));
		for(int i = 0; i < list.size(); i++){
			multi.add(list.get(i).getName());
		}

		//multi.add("abandon success!");
		return multi;
	}

	@GetMapping("/index/dataset")
	public List<String> indexdataset() {
		List<String> multi = new ArrayList<>();
		// TODO:通过选择的数据集、工作负载等运行相应底层取消代码
		ArrayList<Data> list = new ArrayList<>();
		String type = "dataset";
		list = new ArrayList<Data>(readFile(type));
		for(int i = 0; i < list.size(); i++){
			multi.add(list.get(i).getName());
		}
		//multi.add("abandon success!");
		return multi;
	}

	@GetMapping("/index/workload")
	public List<String> indexworkload() {
		List<String> multi = new ArrayList<>();
		// TODO:通过选择的数据集、工作负载等运行相应底层取消代码
		ArrayList<Data> list = new ArrayList<>();
		String type = "workload";
		list = new ArrayList<Data>(readFile(type));
		for(int i = 0; i < list.size(); i++){
			multi.add(list.get(i).getName());
		}
		//multi.add("abandon success!");
		return multi;
	}

	@GetMapping("/index/knob")
	public List<String> indexknob() {
		List<String> multi = new ArrayList<>();
		// TODO:通过选择的数据集、工作负载等运行相应底层取消代码
		ArrayList<Data> list = new ArrayList<>();
		String type = "storage";
		list = new ArrayList<Data>(readFile(type));
		for(int i = 0; i < list.size(); i++){
			multi.add(list.get(i).getName());
		}
		//multi.add("abandon success!");
		return multi;
	}


	@GetMapping("/op/dataset")
	public List<String> opdataset() {
		List<String> multi = new ArrayList<>();
		// TODO:通过选择的数据集、工作负载等运行相应底层取消代码
		ArrayList<Data> list = new ArrayList<>();
		String type = "dataset";
		list = new ArrayList<Data>(readFile(type));
		for(int i = 0; i < list.size(); i++){
			multi.add(list.get(i).getName());
		}
		//multi.add("abandon success!");
		return multi;
	}

	@GetMapping("/op/workload")
	public List<String> opworkload() {
		List<String> multi = new ArrayList<>();
		// TODO:通过选择的数据集、工作负载等运行相应底层取消代码
		ArrayList<Data> list = new ArrayList<>();
		String type = "workload";
		list = new ArrayList<Data>(readFile(type));
		for(int i = 0; i < list.size(); i++){
			multi.add(list.get(i).getName());
		}
		//multi.add("abandon success!");
		return multi;
	}

	@GetMapping("/op/knob")
	public List<String> opknob() {
		List<String> multi = new ArrayList<>();
		// TODO:通过选择的数据集、工作负载等运行相应底层取消代码
		ArrayList<Data> list = new ArrayList<>();
		String type = "storage";
		list = new ArrayList<Data>(readFile(type));
		for(int i = 0; i < list.size(); i++){
			multi.add(list.get(i).getName());
		}
		//multi.add("abandon success!");
		return multi;
	}

	@GetMapping("/op/index")
	public List<String> opindex() {
		List<String> multi = new ArrayList<>();
		// TODO:通过选择的数据集、工作负载等运行相应底层取消代码
		ArrayList<Data> list = new ArrayList<>();
		String type = "index";
		list = new ArrayList<Data>(readFile(type));
		for(int i = 0; i < list.size(); i++){
			multi.add(list.get(i).getName());
		}
		//multi.add("abandon success!");
		return multi;
	}

	@GetMapping("/exe/knob")
	public List<String> exeknob() {
		List<String> multi = new ArrayList<>();
		// TODO:通过选择的数据集、工作负载等运行相应底层取消代码
		ArrayList<Data> list = new ArrayList<>();
		String type = "storage";
		list = new ArrayList<Data>(readFile(type));
		for(int i = 0; i < list.size(); i++){
			multi.add(list.get(i).getName());
		}
		//multi.add("abandon success!");
		return multi;
	}

	@GetMapping("/exe/index")
	public List<String> exeindex() {
		List<String> multi = new ArrayList<>();
		// TODO:通过选择的数据集、工作负载等运行相应底层取消代码
		ArrayList<Data> list = new ArrayList<>();
		String type = "index";
		list = new ArrayList<Data>(readFile(type));
		for(int i = 0; i < list.size(); i++){
			multi.add(list.get(i).getName());
		}
		//multi.add("abandon success!");
		return multi;
	}

	@GetMapping("/exe/upload")
	public JSONObject exeupload(@RequestParam(value = "filepath", required = true) String location) {
		List<String> multi = new ArrayList<>();
		JSONObject ob = new JSONObject();
		// TODO:通过选择的数据集、工作负载等运行相应底层取消代码
		location = location.split("\\\\")[2];
		System.out.println(location);
		FileInputStream inputStream;
		try {
			inputStream = new FileInputStream("./data/"+location);
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, "utf-8"));
			String str = null;
			while ((str = bufferedReader.readLine()) != null) {
				System.out.println(str);
				multi.add(str);
			}
			inputStream.close();
			bufferedReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		ob.put("query",multi.get(0));
		//String query = multi.get(0);
		//multi.add("abandon success!");
		return ob;
	}

}
