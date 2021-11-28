package com.hit.backup.graph;

import java.io.*;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

public class GraphTest2 {

    public ArrayList<String> uploadDataset(String path) throws IOException, InterruptedException {
        ArrayList<String> ret = new ArrayList<>();
        File dataFile = new File(path);
        double fileSizeMB = Long.valueOf(dataFile.length()) / 1024 / 1024;
        FileReader in = new FileReader(dataFile);
        LineNumberReader reader = new LineNumberReader(in);
        reader.skip(Long.MAX_VALUE);
        int lines = reader.getLineNumber();
        String details = "Path:/usr/graph/blog_graph.csv"+"\n"+
                "Type: csv"+"\n"+
                "Count: 609975"+"\n"+
                "Size: 12.694MB"+"\n"+
                "Example:((03787,follow,04088), (08715,follow,04088))";
        ret.add(details);

        int callRet = 0;
//        callRet =
//                callShell(new String[] {"mysql -uroot -p123456 -e 'drop database if exists `before`"});
//        callRet =
//                callShell(new String[] {"mysql -uroot -p123456 -e 'create database if not exists `before`'"});
//        callRet =
//                callShell(new String[] {String.format("mysql -uroot -p123456 before -e 'source %s'", "~/default.sql")});
//        callRet =
//                callShell(new String[] {"mysql -uroot -p123456 -e 'drop database if exists `after`"});
//        callRet = callShell(new String[] {
//                "mysql -uroot -p123456 -e 'create database if not exists " + "`after`'"});
//        callRet =
//                callShell(new String[] {String.format("mysql -uroot -p123456 after -e 'source %s'", "~/default.sql")});

        String configuration = "Name: userlik"+"\n"+
                "Database: MySQL"+"\n"+
                "IP: 127.0.0.1"+"\n"+
                "Port: 50001"+"\n"+
                "Db_name: before, after";

        ret.add(configuration);
        return ret;
    }

    public ArrayList<String> uploadWorkload(String path) throws IOException {
        ArrayList<String> ret = new ArrayList<>();
        File workloadFile = new File(path);
        FileReader in = new FileReader(workloadFile);
        LineNumberReader reader = new LineNumberReader(in);
        reader.skip(Long.MAX_VALUE);
        int lines = reader.getLineNumber();
        String details = "Path：/usr/graph/"+"\n"+
                "Read operation: 20,000"+"\n"+
                "type: read_only"+"\n"+
                "size: 1MB"+"\n"+
                "Example: select a.s, d.o from t0 a, t0 b, t0 c, t0 d where a.p = 'follow' and a.o = '04088' and b.p = 'position' and b.o = 'cleaner' and c.p = 'type' and c.o = 'common user' and d.p = 'name' and a.s = b.s and a.s = c.s and a.s = d.s;";
        ret.add(details);

        String configuration = "Name: workgra_a"+"\n"+
                "IP: 127.0.0.1"+"\n"+
                "Port: 50001"+"\n"+
                "file_name:workload_graph.txt";
        ret.add(configuration);
        return ret;
    }

    private static int callShell(String[] args) throws IOException, InterruptedException {
        Process process = Runtime.getRuntime().exec(args);
        // 获取进程的标准输入流
        final InputStream is1 = process.getInputStream();
        // 获取进城的错误流
        final InputStream is2 = process.getErrorStream();
        // 启动两个线程，一个线程负责读标准输出流，另一个负责读标准错误流
        new Thread() {
            public void run() {
                BufferedReader br1 = new BufferedReader(new InputStreamReader(is1));
                try {
                    String line1 = null;
                    while ((line1 = br1.readLine()) != null) {
                        if (line1 != null) {
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        is1.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }.start();
        new Thread() {
            public void run() {
                BufferedReader br2 = new BufferedReader(new InputStreamReader(is2));
                try {
                    String line2 = null;
                    while ((line2 = br2.readLine()) != null) {
                        if (line2 != null) {
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        is2.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }.start();
        return process.waitFor();
    }
}
