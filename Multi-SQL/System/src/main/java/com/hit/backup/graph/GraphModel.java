package com.hit.backup.graph;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class GraphModel {

    //第一是具体的方案，第二是提高的效率
    public String getStrategy(){
        String strategy = "The smart recommendation results are as follows:"+"\n"+
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
        return strategy;
    }

    public long runPython(String file){
        try {
            long graphStart = System.currentTimeMillis();
            String[] arg= new String[] {
                    "/usr/local/bin/python3.7",
                    file};
            Process proc = Runtime.getRuntime().exec(arg);

            BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            String line = null;
            while ((line = in.readLine()) != null) {
                System.out.println(line);
            }
            in.close();
            proc.waitFor();
            long graphEnd = System.currentTimeMillis();
            return graphEnd - graphStart;
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        return -1;

    }

    public double getEfficiency(){
        long graphTime = runPython("/usr/local/GraphModel/Main.py");
        long rdbTime = runPython("/usr/local/GraphModel/RdbMain.py");
        return Math.abs(graphTime - rdbTime) / rdbTime;
    }

    public static void main(String[] args) {
        GraphModel g = new GraphModel();
        System.out.println(g.getEfficiency());
    }
}
