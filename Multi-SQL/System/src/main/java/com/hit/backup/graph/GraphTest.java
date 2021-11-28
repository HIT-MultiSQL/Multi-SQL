package com.hit.backup.graph;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GraphTest {
    private static final String IP = "172.17.17.3";
    private static String defaultUrl = String.format(
        "jdbc:mysql://%s:3306/graph_default?characterEncoding=UTF-8" + "&serverTimezone=UTC", IP);
    private static String dqnUrl =
        String.format("jdbc:mysql://%s:3306/graph_dqn?characterEncoding=UTF-8&serverTimezone=UTC", IP);
    private static String contrastUrl = String.format(
        "jdbc:mysql://%s:3306/graph_contrast?characterEncoding=UTF-8&serverTimezone" + "=UTC", IP);
    private static String username = "root";
    private static String password = "123456";

    private Map<String, String> dqnRewriteMap;
    private Map<String, String> multiModalMap;
    private Map<String, String> singleModalMap;

    public GraphTest() throws ClassNotFoundException, IOException {
        Class.forName("com.mysql.jdbc.Driver");

        String line = null;
        // 加载智能方法改写映射
        this.dqnRewriteMap = new HashMap<>();
        File dqnRewriteFile =
            new File("/home/wangyutong/dqnRewrite.txt");
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

        this.initMultiModalMap();
        this.initSingleModalMap();
    }

    public Map<String, Double> runtimeTrial() throws SQLException, IOException {
        Connection defaultConn = DriverManager.getConnection(defaultUrl, username, password);

        Connection dqnConn = DriverManager.getConnection(dqnUrl, username, password);
        Connection contrastConn = DriverManager.getConnection(contrastUrl, username, password);
        long startTime, endTime = 0;
        Map<String, Double> timeRecords = new HashMap<>();
        String line = null;
        // 默认方法
        // 1. 读取负载
        File defaultWorkloadFile =
            new File("/home/wangyutong/defaultWorkload.txt");
        FileReader defaultWorkloadFileReader = new FileReader(defaultWorkloadFile);
        BufferedReader defaultWorkloadBufferedReader =
            new BufferedReader(defaultWorkloadFileReader);
        List<String> defaultWorkload = new ArrayList<>();
        while ((line = defaultWorkloadBufferedReader.readLine()) != null) {
            defaultWorkload.add(line);
        }
        defaultWorkloadBufferedReader.close();
        defaultWorkloadFileReader.close();
        // 2. 执行查询
        long defaultTime = 0;
        for (int i = 0; i < defaultWorkload.size(); i++) {
            startTime = System.currentTimeMillis();
            PreparedStatement pst = defaultConn.prepareStatement(defaultWorkload.get(i));
            ResultSet rs = pst.executeQuery();
            endTime = System.currentTimeMillis();
            defaultTime += (endTime - startTime);
        }
        timeRecords.put("default", defaultTime / 1000.0);

        // 智能方法
        // 1. 读取负载
        File dqnWorkloadFile =
            new File("/home/wangyutong/dqnWorkload.txt");
        FileReader dqnWorkloadFileReader = new FileReader(dqnWorkloadFile);
        BufferedReader dqnWorkloadBufferedReader = new BufferedReader(dqnWorkloadFileReader);
        List<String> dqnWorkload = new ArrayList<>();
        while ((line = dqnWorkloadBufferedReader.readLine()) != null) {
            dqnWorkload.add(line);
        }
        dqnWorkloadBufferedReader.close();
        dqnWorkloadFileReader.close();
        // 2. 执行查询
        long dqnTime = 0;
        for (int i = 0; i < dqnWorkload.size(); i++) {
            startTime = System.currentTimeMillis();
            PreparedStatement pst = dqnConn.prepareStatement(dqnWorkload.get(i));
            ResultSet rs = pst.executeQuery();
            endTime = System.currentTimeMillis();
            dqnTime += (endTime - startTime);
        }
        timeRecords.put("dqn", dqnTime / 1000.0);

        // 人工对比方法
        // 1. 读取负载
        File contrastWorkloadFile =
            new File("/home/wangyutong/contrastWorkload.txt");
        FileReader contrastWorkloadFileReader = new FileReader(contrastWorkloadFile);
        BufferedReader contrastWorkloadBufferedReader =
            new BufferedReader(contrastWorkloadFileReader);
        List<String> contrastWorkload = new ArrayList<>();
        while ((line = contrastWorkloadBufferedReader.readLine()) != null) {
            contrastWorkload.add(line);
        }
        contrastWorkloadBufferedReader.close();
        contrastWorkloadFileReader.close();
        // 2. 执行查询
        long contrastTime = 0;
        for (int i = 0; i < contrastWorkload.size(); i++) {
            startTime = System.currentTimeMillis();
            PreparedStatement pst = contrastConn.prepareStatement(contrastWorkload.get(i));
            ResultSet rs = pst.executeQuery();
            endTime = System.currentTimeMillis();
            contrastTime += (endTime - startTime);
        }
        timeRecords.put("contrast", contrastTime / 1000.0);

        defaultConn.close();
        dqnConn.close();
        contrastConn.close();
        return timeRecords;
    }

    public List multiModalTrial(String query) throws SQLException {
        Connection dqnConn = DriverManager.getConnection(dqnUrl, username, password);
        String sql = this.multiModalRewrite(query);
        System.out.println(sql);
        sql = this.dqnRewriteMap.get(sql);
        // 执行查询
        List<String> queryResults = new ArrayList<>();
        System.out.println(sql);
        PreparedStatement pst = dqnConn.prepareStatement(sql);
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            queryResults.add(rs.getString(1));
        }
        dqnConn.close();
        return queryResults;
    }

    public List singleModeTrial(String query) throws SQLException {
        Connection dqnConn = DriverManager.getConnection(dqnUrl, username, password);
        String sql = this.singleModalRewrite(query);
        sql = this.dqnRewriteMap.get(sql);
        System.out.println(sql);
        // 执行查询
        List<String> queryResults = new ArrayList<>();
        PreparedStatement pst = dqnConn.prepareStatement(sql);
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            queryResults.add(rs.getString(1));
        }
        dqnConn.close();
        return queryResults;
    }

    private void initSingleModalMap() {
        this.singleModalMap = new HashMap<>();
        String template1 = "Select a.s from graph a join graph b on a.s = b.s join graph c on b.o"
            + " = c.s join graph d on a.s = d.s where a.p = 'follow' and a.o = 'someID' and b.p = "
            + "'publish' and c.p = 'topic' and c.o = 'someTopic' and d.p = 'gender' and d.o = "
            + "'female';";
        String template2 = "Select a.s from graph a join graph b on a.s = b.s join graph c on a.s"
            + " = c.s where a.p = 'follow' and a.o = 'someID' and b.p = 'position' and b.o = "
            + "'somePosition' and c.p = 'type' and c.o = 'someUser';";
        String template3 = "Select a.s from graph a join graph b on a.s = b.s join graph c on b.o"
            + " = c.s where a.p = 'type' and a.o = 'senior user' and b.p = 'comment' and c.p = "
            + "'topic' and c.o = 'someTopic';";
        String template1_ = "select a.s from t0 a, t0 b, t0 c, t0 d where a.p = 'follow' and a.o "
            + "= 'someID' and b.p = 'publish' and c.p = 'topic' and c.o = 'someTopic' and d.p = "
            + "'gender' and d.o = 'female' and a.s = b.s and b.o = c.s and a.s = d.s;";
        String template2_ = "select a.s from t0 a, t0 b, t0 c where a.p = 'follow' and a.o = "
            + "'someID' and b.p = 'position' and b.o = 'somePosition' and c.p = 'type' and c.o = "
            + "'someUser' and a.s = b.s and a.s = c.s;";
        String template3_ =
            "select a.s from t0 a, t0 b, t0 c where a.p = 'type' and a.o = 'senior user' and b.p "
                + "= 'comment' and c.p = 'topic' and c.o = 'someTopic' and a.s = b.s and b.o = c"
                + ".s;";
        this.singleModalMap.put(template1, template1_);
        this.singleModalMap.put(template2, template2_);
        this.singleModalMap.put(template3, template3_);
    }

    private void initMultiModalMap() {
        this.multiModalMap = new HashMap<>();
        String template1 = "Select gra.s join gra (s = a.s, o = a.o, p = a.p, bp = b.p, bo = "
            + "b.o, cp = c.p, co = c.o, dp = d.p, do = d.o) a, b, c, d a.s = b.s and b.o = c.s "
            + "and a.s = d.s where gra.p = 'follow' and gra.o = 'someID' and gra.bp = 'publish' "
            + "and gra.cp = 'topic' and gra.co = 'someTopic' and gra.dp = 'gender' and gra.do "
            + "= 'female'";
        String template2 =
            "Select gra.s join gra (s = a.s, o = a.o, p = a.p, bp = b.p, bo = b.o, cp = c.p, co = c.o) a, b, c a.s = b.s and a.s = c.s where gra.p = 'follow' and gra.o = 'someID' and "
                + "gra.bp = 'position' and gra.bo = 'somePosition' gra.cp = 'type' and gra.co = "
                + "'someUser'";
        String template3 =
            "Select gra.s join gra (s = a.s, o = a.o, p = a.p, bp = b.p, bo = b.o, cp = c.p, co ="
                + " c.o) a, b, c a.s = b.s and b.o = c.s where gra.p = 'type' and gra.o = 'senior user' and"
                + " gra.bp = 'comment' and gra.cp = 'topic' and gra.co = 'someTopic'";
        String template1_ = "select a.s from t0 a, t0 b, t0 c, t0 d where a.p = 'follow' and a.o "
            + "= 'someID' and b.p = 'publish' and c.p = 'topic' and c.o = 'someTopic' and d.p = "
            + "'gender' and d.o = 'female' and a.s = b.s and b.o = c.s and a.s = d.s;";
        String template2_ = "select a.s from t0 a, t0 b, t0 c where a.p = 'follow' and a.o = "
            + "'someID' and b.p = 'position' and b.o = 'somePosition' and c.p = 'type' and c.o = "
            + "'someUser' and a.s = b.s and a.s = c.s;";
        String template3_ =
            "select a.s from t0 a, t0 b, t0 c where a.p = 'type' and a.o = 'senior user' and b.p "
                + "= 'comment' and c.p = 'topic' and c.o = 'someTopic' and a.s = b.s and b.o = c"
                + ".s;";

        this.multiModalMap.put(template1, template1_);
        this.multiModalMap.put(template2, template2_);
        this.multiModalMap.put(template3, template3_);
    }

    public String singleModalRewrite(String query) {
        Pattern pattern1 = Pattern.compile(
            "a\\.p = 'follow' and a\\.o = '(.*)' and b\\.p = 'publish' and c\\"
                + ".p = 'topic' and c\\.o = '(.*)' and d\\.p = 'gender' and d\\.o = 'female'");
        Pattern pattern2 = Pattern.compile(
            "a\\.p = 'follow' and a\\.o = '(.*)' and b\\.p = 'position' and "
                + "b\\.o = '(.*)' and c\\.p = 'type' and c\\.o = '(.*)'");
        Pattern pattern3 = Pattern.compile(
            "a\\.p = 'type' and a\\.o = 'senior user' and b\\.p = 'comment' "
                + "and c\\.p = 'topic' and c\\.o = '(.*)'");
        Matcher matcher1 = pattern1.matcher(query);
        Matcher matcher2 = pattern2.matcher(query);
        Matcher matcher3 = pattern3.matcher(query);
        String sql = null;
        if (matcher1.find()) {
            String someID = matcher1.group(1);
            String someTopic = matcher1.group(2);
            String tempQuery = query.replace(someTopic, "someTopic").replace(someID, "someID");
            //            System.out.println(tempQuery);
            //            System.out.println(this.singleModalMap.keySet().contains(tempQuery));
            sql =
                this.singleModalMap.get(tempQuery).replace("someTopic", someTopic).replace("someID", someID);
        }
        if (matcher2.find()) {
            String someID = matcher2.group(1);
            String somePosition = matcher2.group(2);
            String someUser = matcher2.group(3);
            String tempQuery =
                query.replace(someID, "someID").replace(somePosition, "somePosition").replace(someUser, "someUser");
            //            System.out.println(tempQuery);
            //            System.out.println(this.singleModalMap.keySet().contains(tempQuery));
            sql =
                this.singleModalMap.get(tempQuery).replace("someID", someID).replace("somePosition", somePosition).replace("someUser", someUser);
        }
        if (matcher3.find()) {
            String someTopic = matcher3.group(1);
            String tempQuery = query.replace(someTopic, "someTopic");
//            System.out.println(tempQuery);
//            System.out.println(this.singleModalMap.keySet().contains(tempQuery));
            sql = this.singleModalMap.get(tempQuery).replace("someTopic", someTopic);
        }
        return sql;
    }

    public String multiModalRewrite(String query) {
    	//this.initMultiModalMap();
        Pattern pattern1 = Pattern.compile("gra\\.p = 'follow' and gra\\.o = '(.*)' and gra\\.bp "
            + "= 'publish' and gra\\.cp = 'topic' and gra\\.co = '(.*)' and gra\\.dp = "
            + "'gender' and gra\\.do = 'female'");
        Pattern pattern2 = Pattern.compile(
            "gra\\.p = 'follow' and gra\\.o = '(.*)' and gra\\.bp = 'position' "
                + "and gra\\.bo = '(.*)' gra\\.cp = 'type' and gra\\.co = '(.*)'");
        Pattern pattern3 = Pattern.compile("gra\\.p = 'type' and gra\\.o = 'senior user' and gra"
            + "\\.bp = 'comment' and gra\\.cp = 'topic' and gra\\.co = '(.*)'");
        Matcher matcher1 = pattern1.matcher(query);
        Matcher matcher2 = pattern2.matcher(query);
        Matcher matcher3 = pattern3.matcher(query);
        String sql = null;
        if (matcher1.find()) {
            String someID = matcher1.group(1);
            String someTopic = matcher1.group(2);
            //            System.out.println(query.replace(someTopic, "someTopic").replace(someID, "someID"));
            //            System.out.println(this.multiModalMap.keySet().contains(query.replace(someTopic, "someTopic").replace(someID, "someID")));
            sql =
                this.multiModalMap.get(query.replace(someTopic, "someTopic").replace(someID, "someID")).replace("someTopic", someTopic).replace("someID", someID);
        }
        if (matcher2.find()) {
            String someID = matcher2.group(1);
            String somePosition = matcher2.group(2);
            String someUser = matcher2.group(3);
            //            System.out.println(query.replace(someID, "someID").replace(somePosition, "somePosition").replace(someUser, "someUser"));
            //            System.out.println(this.multiModalMap.keySet().contains(query.replace(someID, "someID").replace(somePosition, "somePosition").replace(someUser, "someUser")));
            sql =
                this.multiModalMap.get(query.replace(someID, "someID").replace(somePosition, "somePosition").replace(someUser, "someUser")).replace("someID", someID).replace("somePosition", somePosition).replace("someUser", someUser);
        }
        if (matcher3.find()) {
            String someTopic = matcher3.group(1);
            //            System.out.println(query.replace(someTopic, "someTopic"));
            //            System.out.println(this.multiModalMap.keySet().contains(query.replace(someTopic, "someTopic")));
            sql =
                this.multiModalMap.get(query.replace(someTopic, "someTopic")).replace("someTopic", someTopic);
        }
        return sql;
    }
}
