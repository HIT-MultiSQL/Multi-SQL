package doc;

import java.io.BufferedReader;  
import java.io.IOException;  
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Arrays;


public class QtFunc {
	
	private String dbname = "test";
	private String collection = "traincoll";
	private String python = "/usr/local/bin/python3.7";
	//private String python = "/bin/python";
	
	private String src = System.getProperty("user.dir");
	//private String src = "/home/mult";
	private int dataNum = 1;
	private int workloadNum = 1;
	public List<String> uploadData(String path) {
		List<String> res = this.runInPython(src+"/src/main/python/data_load.py", new String[]{src+path,"data"+dataNum,this.dbname,this.collection});
		dataNum += 1;
		return res;
	}
	
	public List<String> uploadWorkload(String path) {
		List<String> res = this.runInPython(src+"/src/main/python/workload_load.py", new String[]{src+path,"workload"+workloadNum});
		workloadNum += 1;
		return res;
	}
	private String getPath(String path) {
		Pattern r = Pattern.compile("path:(.*)\n");
	    Matcher m = r.matcher(path);
	    if(m.find()) {
	    	return m.group(1);
	    }
	    return null;
	}
	
	public List<String> indexSelect(List<String> dataDetials, List<String> workloadDetials){
		String datapath = getPath(dataDetials.get(0));
		String workloadpath = getPath(workloadDetials.get(0));
		String field = workloadDetials.get(2);
//		System.out.println(field);
		String[] fields = field.split(",");
		List<String> allFields = new ArrayList<String>(Arrays.asList(new String[] {"id", "authorid", "topic", "date", "blog.title", "blog.key words", "blog.content"}));
		int num  = fields.length;
		List<String> res = new ArrayList<String>();
		List<String> finalRes = new ArrayList<String>();
		double rate = 0; 
		for(int i = 0;i < num;i++) {
			String f = fields[i];
			String[] document = f.split("\\.");
			List<String> r = this.runInPython(src+"/src/main/python/index_select.py", new String[]{datapath,workloadpath,document[0].trim(),document[1].trim(),src,this.dbname,this.collection});
			allFields.remove(document[1].trim());
			res.add(r.get(1));
			rate = Double.parseDouble(r.get(2));
		}
		String resString = "The recommended index of fields under this load is as follows:\n";
		for(String i:res) {
			resString += '\t' + i ;
		}
		for(String i:allFields) {
			resString += '\t' + i +"\n\t\t"+"no index\n";
		}
		resString +=  "Smart method indexing performance increased by" + Double.toString(rate) + "%";
		finalRes.add(resString);
		finalRes.add(Double.toString(rate));
		return finalRes;
	}
	
	public List<String> runInPython(String local, String[] args) {
        List<String> res = new ArrayList<String>();
		try {
            //"/home/huan/myfile/pythonfile/helloword.py"
            int n = args.length+2;
            String[] args1 = new String[n];
            args1[0] = python;
            args1[1] = local;
            
            for(int i=2;i<n;i++) 
            {
            	args1[i] = args[i-2];
            }
            
            
            for(String line:args1) {
            	System.out.println(line);
            }
            
            
            Process pr=Runtime.getRuntime().exec(args1);
 
            BufferedReader in = new BufferedReader(new InputStreamReader(
                    pr.getInputStream()));
            String resString = "";
            String line;
            while ((line = in.readLine()) != null) {
            	if(line.equals("block")) {
            		res.add(resString);
            		resString = "";
            	}
            	else
            		resString += line + "\n";
            	//System.out.println(line);
            }
            res.add(resString);
            in.close();
            pr.waitFor();
        } catch (Exception e) {
            e.printStackTrace();
        }	
		return res;
	}
	
	public static void main(String[] args) {
		QtFunc a = new QtFunc();
		List<String> res = a.uploadWorkload("/workload/workload_dox");
		System.out.print(res.get(0));
		System.out.print(res.get(1));
		System.out.println("");
		
		
		List<String> res2 = a.uploadData("/dataset/blog_dox.json");
		System.out.print(res2.get(0));
		System.out.print(res2.get(1));
		System.out.print(res2.get(2));
		System.out.println("");
		
		
		// index+ran; num
		List<String> res3 = a.indexSelect(res2, res);
		System.out.print(res3.get(0));
		System.out.print(res3.get(1));
		System.out.println("");
		
		
		}
	
}
