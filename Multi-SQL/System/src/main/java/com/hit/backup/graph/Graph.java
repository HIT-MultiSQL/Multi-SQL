package com.hit.backup.graph;

import java.io.*;
import java.util.*;

public class Graph {

    private final String PYTHON_HOME = "/usr/local/bin/python3.7"; // python
    private Map<String, String> dqnRewriteMap;

    /**
     * 查询部分query，根据时间计算智能方法快多少。
     * @return Double List, 第一个位置为智能方法比default方法快多少，第二个位置为比人工方法快多少（小数形式）
     */

    public Graph() throws ClassNotFoundException, IOException{

        String line = null;

        this.dqnRewriteMap = new HashMap<>();
        File dqnRewriteFile =
                new File("/dqnx.txt");
        FileReader dqnRewriteFileReader = new FileReader(dqnRewriteFile);
        BufferedReader dqnRewriteBufferedReader = new BufferedReader(dqnRewriteFileReader);
        while ((line = dqnRewriteBufferedReader.readLine()) != null) {
            if (line.isEmpty()) {
                continue;
            }
            String[] item = line.split("\\|");
            this.dqnRewriteMap.put(item[0], item[1]);
        }
        dqnRewriteBufferedReader.close();
        dqnRewriteFileReader.close();
    }

    private List<Double> execute6() {
        try {
            //String[] arg= new String[] { PYTHON_HOME, "/GraphModelSimple/execute6.py"};
            String arg = PYTHON_HOME+"/GraphModelSimple/execute6.py";
            Process proc = Runtime.getRuntime().exec(arg);
            BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            String line = in.readLine();
            String[] result = line.split(",");
            List<Double> times = new ArrayList<>();
            times.add(Double.parseDouble(result[0]));
            times.add(Double.parseDouble(result[1]));
            in.close();
            proc.waitFor();
            return times;
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return new ArrayList<>();
    }

    /**
     * 查询固定的11个query，返回结果。
     * @param query 需要查询的query
     * @return 返回的结果List
     */
    private List<List<String>> execute9(String query) {
        try {
            //String[] arg= new String[] { PYTHON_HOME, "/GraphModelSimple/execute9.py", query};
            String arg = PYTHON_HOME+"/GraphModelSimple/execute9.py "+query;
            Process proc = Runtime.getRuntime().exec(arg);
            BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            String line = null;
            List<List<String>> allResult = new ArrayList<>();
            while ((line = in.readLine()) != null) {
                line = line.substring(1, line.length() - 1);
                String[] splitLine = line.split(",");
                List<String> oneLineResult = Arrays.asList(splitLine);
                allResult.add(oneLineResult);
            }
            in.close();
            proc.waitFor();
            return allResult;
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return new ArrayList<>();
    }

    /**
     * 查询图数据库并且返回结果
     //* @param query 查询语句
     * @return 返回的查询结果
     */


    public ArrayList<String> getresult() {
        ArrayList<String> result = new ArrayList<String>();
        try {

            File f1 = new File("/GraphModelSimple/result.txt");
            InputStreamReader reader1;

            reader1 = new InputStreamReader(new FileInputStream(f1), "utf-8");
            BufferedReader br1 = new BufferedReader(reader1);
            String str = null;
            while ((str = br1.readLine()) != null) {
                // System.out.println(str);
                result.add(str);
            }
            br1.close();
            reader1.close();
            return result;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return result;
    }

    public List<String> graphQuery(String query) {
        String sql = null;
        sql = dqnRewriteMap.get(query);
        System.out.println(sql);
        try {
            String[] arg= new String[] { PYTHON_HOME, "/GraphModelSimple/execute114.py", sql};
            //String arg = PYTHON_HOME+"/GraphModelSimple/GraphQuery.py "+sql;
            //long starttime = System.currentTimeMillis();
            Process proc = Runtime.getRuntime().exec(arg);
//            long endtime = System.currentTimeMillis();
//            long end = endtime-starttime;
//            String ttt = end + "ms";
//            System.out.println(end);
            List<String> allResult = new ArrayList<>(getresult());

//            File result =
//                    new File("/GraphModelSimple/result.txt");
//            FileReader graresult = new FileReader(result);
//            BufferedReader grabuffer = new BufferedReader(graresult);


//            BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
//            String line = null;
//            List<String> allResult = new ArrayList<>();
//            while ((line = in.readLine()) != null) {
//                line = line.substring(1, line.length() - 1);
//                String[] splitLine = line.split(",");
//                //List<String> oneLineResult = Arrays.asList(splitLine);
//                allResult.add(splitLine[0]);
//            }
//            //System.out.println(allResult);
//            in.close();
//            proc.waitFor();

            return allResult;
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return new ArrayList<>();
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException {
        Graph test = new Graph();
        String query = "match(n) return n;";
        test.graphQuery(query);

    }

}
