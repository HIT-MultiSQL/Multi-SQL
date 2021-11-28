package com.relational;

import com.datastax.driver.core.*;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.relational.IOStrategy.BufferStrategy;
import com.relational.IOStrategy.IOInterface;
import sun.nio.ch.IOStatus;

import java.io.*;
import java.net.URL;
import java.net.URLDecoder;
import java.sql.*;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.Date;
import java.util.concurrent.*;

public class Execution {

    Session session = null;
    Connection mysqlConnection = null;
    com.datastax.driver.core.Session cassandrasession = null;
    Cluster cluster = null;

    /**
     * create database connection
     */
    public Execution() {
        try {
//            String Sshuser = "yanhao";
//            String SShPassword = "yh949514033";
//            String SSHip = "219.217.229.74";
//            int SShPort = 22;
//            JSch jsch = new JSch();
//            Session session = jsch.getSession(Sshuser, SSHip, SShPort);
//            session.setPassword(SShPassword);
//            session.setConfig("StrictHostKeyChecking", "no");
//            session.connect();//连接
//            System.out.println(session.getServerVersion());//这里打印SSH服务器版本信息
//            this.session = session;
//
//            //正向代理
//            int assinged_port_mysql = session.setPortForwardingL(3309, "0.0.0.0", 50000);//mysql端口映射 转发
//            System.out.println("mysql端口:" + assinged_port_mysql);
//            int assinged_port_cassa = session.setPortForwardingL(3311, "219.217.229.74", 9042);//cassandra端口映射 转发
//            System.out.println("cassandra端口:" + assinged_port_cassa);

            String driverClass = null;
            String url = null;
            String user = null;
            String password = null;
            //第一步：准备连接数据库的4个参数：驱动名、jdbcUrl,user,password
            //1.读取配置盘文件
            Properties properties = new Properties();
            FileInputStream fileInputStream = null;
            fileInputStream = new FileInputStream(new File("mysql.properties"));
            properties.load(fileInputStream);
            fileInputStream.close();
            //2.从配置文件中获取读取的参数值
            driverClass = properties.getProperty("driverclass");
            url = properties.getProperty("url");
            user = properties.getProperty("user");
            password = properties.getProperty("password");
            //第二步：加载驱动
            Class.forName(driverClass);
            //第三步：通过DriverManager的getconnection方法获取数据连接
            Connection connection = DriverManager.getConnection(url, user, password);
            this.mysqlConnection = connection;

            this.cluster = Cluster.builder().addContactPoints("0.0.0.0").withPort(7042).build();
            this.cassandrasession = cluster.connect("user_1");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * close database connection
     */
    public void close() {
        try {
            this.mysqlConnection.close();
//            this.session.disconnect();
            this.cluster.close();
            this.cassandrasession.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * single mode query function. mode=1 represents default, mode=2 represents intelligence
     * , mode=3 represents artificial.
     *
     * @param sql  query sql
     * @param mode query mode
     * @return query result
     */
    public List<String> singleModeQuery(String sql, int mode) {
        if (mode == 0) {
            try {
                Statement st = this.mysqlConnection.createStatement();
                List<String> results = new ArrayList<String>();
                ResultSet resultSet = null;
                if (sql.contains("select activity.device, count(activity.device) ")) {
                    String start = sql.substring(sql.indexOf("between") + 8, sql.indexOf("between") + 16);
                    String end = sql.substring(sql.indexOf("between") + 21, sql.indexOf("between") + 29);
                    String v = sql.substring(sql.indexOf("[") + 1, sql.indexOf("]"));
                    String[] uids = v.split(",");
                    if (uids.length != 0) {
                        StringBuilder sb = new StringBuilder();
                        sb.append("uid = " + uids[0]);
                        int i = 1;
                        while (i < uids.length) {
                            sb.append(" or uid = " + uids[i]);
                            i++;
                        }
                        resultSet = st.executeQuery("select device, count(device) from activity " +
                                "where log_time between " + start + " and " + end + " and (" +
                                sb.toString() + ") group by device");
                    }
                } else if (sql.contains("select basicinfo.id from basicinfo join activity")) {
                    String register_date = sql.substring(sql.indexOf("<") + 2, sql.indexOf("<") + 10);
                    String log_time = sql.substring(sql.indexOf(";") - 8, sql.indexOf(";"));
                    String v = sql.substring(sql.indexOf("[") + 1, sql.indexOf("]"));
                    String[] uids = v.split(",");
                    if (uids.length != 0) {
                        StringBuilder sb = new StringBuilder();
                        sb.append("uid = " + uids[0]);
                        int i = 1;
                        while (i < uids.length) {
                            sb.append(" or uid = " + uids[i]);
                            i++;
                        }
                        resultSet = st.executeQuery("select basicinfo.id from basicinfo join activity on basicinfo.id = activity.uid " +
                                "where (" + sb.toString() + ") and register_date < " + register_date + " and log_time = " + log_time);
                    }

                } else if (sql.contains("select * ")) {
                    String register_date = sql.substring(sql.indexOf("<") + 2, sql.indexOf("<") + 10);
                    String v = sql.substring(sql.indexOf("[") + 1, sql.indexOf("]"));
                    String[] uids = v.split(",");
                    if (uids.length != 0) {
                        StringBuilder sb = new StringBuilder();
                        sb.append("id = " + uids[0]);
                        int i = 1;
                        while (i < uids.length) {
                            sb.append(" or id = " + uids[i]);
                            i++;
                        }
                        String c = "select * from basicinfo where (" + sb.toString() + ") and register_date < " + register_date;
                        resultSet = st.executeQuery("select * from basicinfo where (" + sb.toString() + ") and register_date < " + register_date);
                    }
                } else if (sql.contains("select basicinfo.id from basicinfo where")) {
                    String v = sql.substring(sql.indexOf("[") + 1, sql.indexOf("]"));
                    String[] ids = v.split(",");
                    if (ids.length != 0) {
                        StringBuilder sb = new StringBuilder();
                        sb.append("id = " + ids[0]);
                        int i = 1;
                        while (i < ids.length) {
                            sb.append(" or id = " + ids[i]);
                            i++;
                        }
                        resultSet = st.executeQuery("select id from basicinfo where (" + sb.toString() + ") and age >= 20 and age <= 30");
                    }
                } else if (sql.contains("insert into activity")) {
                    String nsql = sql.substring(0, sql.length() - 1);
                    st.execute(nsql);
                }
                if (resultSet != null) {
                    int count = resultSet.getMetaData().getColumnCount();
                    while (resultSet.next()) {
                        StringBuilder sb = new StringBuilder();
                        for (int i = 1; i <= count; i++) {
                            sb.append(resultSet.getString(i));
                            if (i < count) {
                                sb.append(" ");
                            }
                        }
                        results.add(sb.toString());
                    }
                }
                st.close();
                return results;
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (mode == 1) {
            try {
                Statement mysqlConnectionStatement = this.mysqlConnection.createStatement();
                List<String> results = new ArrayList<String>();
                if (sql.contains("select activity.device, count(activity.device) ")) {
                    String start = sql.substring(sql.indexOf("between") + 8, sql.indexOf("between") + 16);
                    String end = sql.substring(sql.indexOf("between") + 21, sql.indexOf("between") + 29);
                    String v = sql.substring(sql.indexOf("[") + 1, sql.indexOf("]"));
                    String start_year = start.substring(0, 4);
                    String start_month = start.substring(4, 6);
                    String start_day = start.substring(6, 8);
                    String end_year = end.substring(0, 4);
                    String end_month = end.substring(4, 6);
                    String end_day = end.substring(6, 8);
                    String[] uids = v.split(",");
                    if (uids.length != 0) {
                        Map<String, Integer> map = new HashMap<String, Integer>();
                        for (String uid : uids) {
                            com.datastax.driver.core.ResultSet resultSet_cassandra = cassandrasession.execute("select device from activity " +
                                    "where log_time >= '" + start_year + "-" + start_month + "-" + start_day + "' " +
                                    "and log_time <= '" + end_year + "-" + end_month + "-" + end_day + "' and (uid = " +
                                    uid + ") allow filtering");
                            if (resultSet_cassandra != null) {
                                for (Row row : resultSet_cassandra) {
                                    if (!map.containsKey(row.getString(0))) {
                                        map.put(row.getString(0), 1);
                                    } else {
                                        map.put(row.getString(0), map.get(row.getString(0)).intValue() + 1);
                                    }
                                }
                            }
                        }
                        for (String s : map.keySet()) {
                            results.add(s + " " + map.get(s));
                        }
                    }
                } else if (sql.contains("select basicinfo.id from basicinfo join activity")) {
                    String register_date = sql.substring(sql.indexOf("<") + 2, sql.indexOf("<") + 10);
                    String log_time = sql.substring(sql.indexOf(";") - 8, sql.indexOf(";"));
                    String v = sql.substring(sql.indexOf("[") + 1, sql.indexOf("]"));
                    String[] ids = v.split(",");
                    if (ids.length != 0) {
                        StringBuilder sb = new StringBuilder();
                        sb.append("id = " + ids[0]);
                        int i = 1;
                        while (i < ids.length) {
                            sb.append(" or id = " + ids[i]);
                            i++;
                        }
                        ResultSet resultSet1 = mysqlConnectionStatement.executeQuery("select id from basicinfo where " +
                                "register_date < " + register_date + " and (" + sb.toString() + ")");
                        List<String> result1 = new ArrayList<String>();
                        if (resultSet1 != null) {
                            int count = resultSet1.getMetaData().getColumnCount();
                            while (resultSet1.next()) {
                                sb = new StringBuilder();
                                for (i = 1; i <= count; i++) {
                                    sb.append(resultSet1.getString(i));
                                    if (i < count) {
                                        sb.append(" ");
                                    }
                                }
                                result1.add(sb.toString());
                            }
                        }
                        String year = log_time.substring(0, 4);
                        String month = log_time.substring(4, 6);
                        String day = log_time.substring(6, 8);
                        for (String uid : ids) {
                            com.datastax.driver.core.ResultSet resultSet2 = cassandrasession.execute("select uid from activity " +
                                    "where log_time = '" + year + "-" + month + "-" + day + "' and " +
                                    "(uid = " + uid + ") allow filtering");
                            if (resultSet2 != null) {
                                for (Row row : resultSet2) {
                                    if (result1.contains(new Integer(row.getString(0)).toString())) {
                                        results.add(new Integer(row.getString(0)).toString());
                                    }
                                }
                            }
                        }
                        mysqlConnectionStatement.close();
                        return results;
                    }
                } else if (sql.contains("select * ")) {
                    String register_date = sql.substring(sql.indexOf("<") + 2, sql.indexOf("<") + 10);
                    String v = sql.substring(sql.indexOf("[") + 1, sql.indexOf("]"));
                    String[] uids = v.split(",");
                    if (uids.length != 0) {
                        StringBuilder sb = new StringBuilder();
                        sb.append("id = " + uids[0]);
                        int i = 1;
                        while (i < uids.length) {
                            sb.append(" or id = " + uids[i]);
                            i++;
                        }
                        ResultSet resultSet = mysqlConnectionStatement.executeQuery("Select * from basicinfo where (" + sb.toString() + ") and register_date < " + register_date);
                        if (resultSet != null) {
                            int count = resultSet.getMetaData().getColumnCount();
                            while (resultSet.next()) {
                                sb = new StringBuilder();
                                for (i = 1; i <= count; i++) {
                                    sb.append(resultSet.getString(i));
                                    if (i < count) {
                                        sb.append(" ");
                                    }
                                }
                                results.add(sb.toString());
                            }
                        }
                    }
                } else if (sql.contains("select basicinfo.id from basicinfo where")) {
                    String v = sql.substring(sql.indexOf("[") + 1, sql.indexOf("]"));
                    String[] ids = v.split(",");
                    if (ids.length != 0) {
                        StringBuilder sb = new StringBuilder();
                        sb.append("id = " + ids[0]);
                        int i = 1;
                        while (i < ids.length) {
                            sb.append(" or id = " + ids[i]);
                            i++;
                        }
                        ResultSet resultSet = mysqlConnectionStatement.executeQuery("select id from basicinfo where (" + sb.toString() + ") and age >= 20 and age <= 30");
                        if (resultSet != null) {
                            int count = resultSet.getMetaData().getColumnCount();
                            while (resultSet.next()) {
                                sb = new StringBuilder();
                                for (i = 1; i <= count; i++) {
                                    sb.append(resultSet.getString(i));
                                    if (i < count) {
                                        sb.append(" ");
                                    }
                                }
                                results.add(sb.toString());
                            }
                        }
                    }
                } else if (sql.contains("insert into activity")) {
                    String value = sql.substring(sql.indexOf("("), sql.length() - 1);
                    String[] tmp = value.split(",");
                    String log_time = tmp[2];
                    String year = log_time.substring(1, 5);
                    String month = log_time.substring(5, 7);
                    String day = log_time.substring(7, 9);
                    cassandrasession.execute("insert into activity(id, uid, log_time, ip, device) values " +
                            tmp[0] + "," + tmp[1] + ",'" + year + "-" + month + "-" + day + "'," + tmp[3] + "," + tmp[4]);
                }
                mysqlConnectionStatement.close();
                return results;
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (mode == 2) {
            try {
                List<String> results = new ArrayList<String>();
                if (sql.contains("select activity.device, count(activity.device) ")) {
                    String start = sql.substring(sql.indexOf("between") + 8, sql.indexOf("between") + 16);
                    String end = sql.substring(sql.indexOf("between") + 21, sql.indexOf("between") + 29);
                    String v = sql.substring(sql.indexOf("[") + 1, sql.indexOf("]"));
                    String start_year = start.substring(0, 4);
                    String start_month = start.substring(4, 6);
                    String start_day = start.substring(6, 8);
                    String end_year = end.substring(0, 4);
                    String end_month = end.substring(4, 6);
                    String end_day = end.substring(6, 8);
                    String[] uids = v.split(",");
                    if (uids.length != 0) {
                        Map<String, Integer> map = new HashMap<String, Integer>();
                        for (String uid : uids) {
                            com.datastax.driver.core.ResultSet resultSet_cassandra = cassandrasession.execute("select device from activity " +
                                    "where log_time >= '" + start_year + "-" + start_month + "-" + start_day + "' " +
                                    "and log_time <= '" + end_year + "-" + end_month + "-" + end_day + "' and (uid = " +
                                    uid + ") allow filtering");
                            if (resultSet_cassandra != null) {
                                for (Row row : resultSet_cassandra) {
                                    if (!map.containsKey(row.getString(0))) {
                                        map.put(row.getString(0), 1);
                                    } else {
                                        map.put(row.getString(0), map.get(row.getString(0)).intValue() + 1);
                                    }
                                }
                            }
                        }
                        for (String s : map.keySet()) {
                            results.add(s + " " + map.get(s));
                        }
                    }
                } else if (sql.contains("select basicinfo.id from basicinfo join activity")) {
                    String register_date = sql.substring(sql.indexOf("<") + 2, sql.indexOf("<") + 10);
                    String log_time = sql.substring(sql.indexOf(";") - 8, sql.indexOf(";"));
                    String v = sql.substring(sql.indexOf("[") + 1, sql.indexOf("]"));
                    String[] ids = v.split(",");
                    if (ids.length != 0) {
                        String year = register_date.substring(0, 4);
                        String month = register_date.substring(4, 6);
                        String day = register_date.substring(6, 8);
                        List<String> result1 = new ArrayList<String>();
                        for (String id : ids) {
                            com.datastax.driver.core.ResultSet resultSet1 = cassandrasession.execute("select id from basicinfo where " +
                                    "register_date < '" + year + "-" + month + "-" + day + "' and (id = " + id + ") allow filtering");
                            if (resultSet1 != null) {
                                for (Row row : resultSet1) {
                                    result1.add(new Integer(row.getString(0)).toString());
                                }
                            }
                        }
                        year = log_time.substring(0, 4);
                        month = log_time.substring(4, 6);
                        day = log_time.substring(6, 8);
                        for (String uid : ids) {
                            com.datastax.driver.core.ResultSet resultSet2 = cassandrasession.execute("select uid from activity " +
                                    "where log_time = '" + year + "-" + month + "-" + day + "' and " +
                                    "(uid = " + uid + ") allow filtering");
                            if (resultSet2 != null) {
                                for (Row row : resultSet2) {
                                    if (result1.contains(new Integer(row.getString(0)).toString())) {
                                        results.add(new Integer(row.getString(0)).toString());
                                    }
                                }
                            }
                        }
                        return results;
                    }
                } else if (sql.contains("select * ")) {
                    String register_date = sql.substring(sql.indexOf("<") + 2, sql.indexOf("<") + 10);
                    String v = sql.substring(sql.indexOf("[") + 1, sql.indexOf("]"));
                    String year = register_date.substring(0, 4);
                    String month = register_date.substring(4, 6);
                    String day = register_date.substring(6, 8);
                    String[] ids = v.split(",");
                    if (ids.length != 0) {
                        for (String id : ids) {
                            com.datastax.driver.core.ResultSet resultSet = cassandrasession.execute("Select * from basicinfo where (id = " + id + ") " +
                                    "and register_date < '" + year + "-" + month + "-" + day + "' allow filtering");
                            if (resultSet != null) {
                                for (Row row : resultSet) {
                                    results.add(row.getString(0) + " " + row.getString(3) + " " +
                                            row.getString(1) + " " + row.getString(2) + " " + row.getString(4)
                                            + " " + row.getDate(5).toString());
                                }
                            }
                        }
                    }
                } else if (sql.contains("select basicinfo.id from basicinfo where")) {
                    String v = sql.substring(sql.indexOf("[") + 1, sql.indexOf("]"));
                    String[] ids = v.split(",");
                    if (ids.length != 0) {
                        for (String id : ids) {
                            com.datastax.driver.core.ResultSet resultSet = cassandrasession.execute("select id from basicinfo where (id = " + id + ") and age >= 20 and age <= 30 allow filtering");
                            for (Row row : resultSet) {
                                results.add(new Integer(row.getString(0)).toString());
                            }
                        }
                    }
                } else if (sql.contains("insert into activity")) {
                    String value = sql.substring(sql.indexOf("("), sql.length() - 1);
                    String[] tmp = value.split(",");
                    String log_time = tmp[2];
                    String year = log_time.substring(1, 5);
                    String month = log_time.substring(5, 7);
                    String day = log_time.substring(7, 9);
                    cassandrasession.execute("insert into activity(id, uid, log_time, ip, device) values " +
                            tmp[0] + "," + tmp[1] + ",'" + year + "-" + month + "-" + day + "'," + tmp[3] + "," + tmp[4]);
                }
                return results;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    /**
     * multi mode query function. mode=1 represents default, mode=2 represents intelligence
     * , mode=3 represents artificial.
     *
     * @param sql  query sql
     * @param mode query mode
     * @return query result
     */
    public List<String> multiModeQuery(String sql, int mode) {
        if (mode == 0) {
            try {
                Statement st = mysqlConnection.createStatement();
                List<String> results = new ArrayList<String>();
                ResultSet resultSet = null;
                if (sql.contains("select activity.device")) {
                    String start = sql.substring(sql.indexOf("between") + 8, sql.indexOf("between") + 16);
                    String end = sql.substring(sql.indexOf("between") + 21, sql.indexOf("between") + 29);
                    String v = sql.substring(sql.indexOf("[") + 1, sql.indexOf("]"));
                    String[] uids = v.split(",");
                    if (uids.length != 0) {
                        StringBuilder sb = new StringBuilder();
                        sb.append("uid = " + uids[0]);
                        int i = 1;
                        while (i < uids.length) {
                            sb.append(" or uid = " + uids[i]);
                            i++;
                        }
                        resultSet = st.executeQuery("select device, count(device) from activity " +
                                "where log_time between " + start + " and " + end + " and (" +
                                sb.toString() + ") group by device");
                    }
                } else if (sql.contains("Select rel.id where")) {
                    String register_date = sql.substring(sql.indexOf("<") + 2, sql.indexOf("<") + 8);
                    String log_time = sql.substring(sql.indexOf(";") - 8, sql.indexOf(";"));
                    if (sql.contains(">")) {
                        if (sql.contains(" rel.id in")) {
                            String v = sql.substring(sql.indexOf("[") + 1, sql.indexOf("]"));
                            String[] ids = v.split(",");
                            if (ids.length != 0) {
                                StringBuilder sb = new StringBuilder();
                                sb.append("id = " + ids[0]);
                                int i = 1;
                                while (i < ids.length) {
                                    sb.append(" or id = " + ids[i]);
                                    i++;
                                }
                                resultSet = st.executeQuery("select id from basicinfo join activity on basicinfo.id = activity.uid" +
                                        " where register_date < " + register_date + " and log_time >= " + log_time + " and (" + sb.toString() + ")");
                            }
                        } else {
                            resultSet = st.executeQuery("select id from basicinfo join activity on basicinfo.id = activity.uid" +
                                    " where register_date < " + register_date + " and log_time >= " + log_time);
                        }
                    } else {
                        resultSet = st.executeQuery("select id from basicinfo join activity on basicinfo.id = activity.uid" +
                                " where register_date < " + register_date + " and log_time = " + log_time);
                    }
                } else if (sql.contains("Select rel.*")) {
                    String register_date = sql.substring(sql.indexOf("<") + 2, sql.indexOf("<") + 10);
                    String v = sql.substring(sql.indexOf("[") + 1, sql.indexOf("]"));
                    String[] uids = v.split(",");
                    if (uids.length != 0) {
                        StringBuilder sb = new StringBuilder();
                        sb.append("id = " + uids[0]);
                        int i = 1;
                        while (i < uids.length) {
                            sb.append(" or id = " + uids[i]);
                            i++;
                        }
                        String c = "select * from basicinfo where (" + sb.toString() + ") and register_date < " + register_date;
                        resultSet = st.executeQuery("select * from basicinfo where (" + sb.toString() + ") and register_date < " + register_date);
                    }
                } else if (sql.equals("Select rel.id where rel.age = 20")) {
                    resultSet = st.executeQuery("select id from basicinfo where age = 20");
                }
                if (resultSet != null) {
                    int count = resultSet.getMetaData().getColumnCount();
                    while (resultSet.next()) {
                        StringBuilder sb = new StringBuilder();
                        for (int i = 1; i <= count; i++) {
                            sb.append(resultSet.getString(i));
                            if (i < count) {
                                sb.append(" ");
                            }
                        }
                        results.add(sb.toString());
                    }
                }
                st.close();
                return results;
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (mode == 1) {
            try {
                Statement mysqlConnectionStatement = mysqlConnection.createStatement();
                List<String> results = new ArrayList<String>();
                if (sql.contains("select activity.device")) {
                    String start = sql.substring(sql.indexOf("between") + 8, sql.indexOf("between") + 16);
                    String end = sql.substring(sql.indexOf("between") + 21, sql.indexOf("between") + 29);
                    String v = sql.substring(sql.indexOf("[") + 1, sql.indexOf("]"));
                    String start_year = start.substring(0, 4);
                    String start_month = start.substring(4, 6);
                    String start_day = start.substring(6, 8);
                    String end_year = end.substring(0, 4);
                    String end_month = end.substring(4, 6);
                    String end_day = end.substring(6, 8);
                    String[] uids = v.split(",");
                    if (uids.length != 0) {
                        Map<String, Integer> map = new HashMap<String, Integer>();
                        for (String uid : uids) {
                            com.datastax.driver.core.ResultSet resultSet_cassandra = cassandrasession.execute("select device from activity " +
                                    "where log_time >= '" + start_year + "-" + start_month + "-" + start_day + "' " +
                                    "and log_time <= '" + end_year + "-" + end_month + "-" + end_day + "' and (uid = " +
                                    uid + ") allow filtering");
                            if (resultSet_cassandra != null) {
                                for (Row row : resultSet_cassandra) {
                                    if (!map.containsKey(row.getString(0))) {
                                        map.put(row.getString(0), 1);
                                    } else {
                                        map.put(row.getString(0), map.get(row.getString(0)).intValue() + 1);
                                    }
                                }
                            }
                        }
                        for (String s : map.keySet()) {
                            results.add(s + " " + map.get(s));
                        }
                    }
                } else if (sql.contains("Select rel.id where")) {
                    String register_date = sql.substring(sql.indexOf("<") + 2, sql.indexOf("<") + 10);
                    String log_time = sql.substring(sql.indexOf(";") - 8, sql.indexOf(";"));
                    String v = sql.substring(sql.indexOf("[") + 1, sql.indexOf("]"));
                    String[] ids = v.split(",");
                    if (ids.length != 0) {
                        StringBuilder sb = new StringBuilder();
                        sb.append("id = " + ids[0]);
                        int i = 1;
                        while (i < ids.length) {
                            sb.append(" or id = " + ids[i]);
                            i++;
                        }
                        ResultSet resultSet1 = mysqlConnectionStatement.executeQuery("select id from basicinfo where " +
                                "register_date < " + register_date + " and (" + sb.toString() + ")");
                        List<String> result1 = new ArrayList<String>();
                        if (resultSet1 != null) {
                            int count = resultSet1.getMetaData().getColumnCount();
                            while (resultSet1.next()) {
                                sb = new StringBuilder();
                                for (i = 1; i <= count; i++) {
                                    sb.append(resultSet1.getString(i));
                                    if (i < count) {
                                        sb.append(" ");
                                    }
                                }
                                result1.add(sb.toString());
                            }
                        }
                        String year = log_time.substring(0, 4);
                        String month = log_time.substring(4, 6);
                        String day = log_time.substring(6, 8);
                        for (String uid : ids) {
                            com.datastax.driver.core.ResultSet resultSet2 = cassandrasession.execute("select uid from activity " +
                                    "where log_time = '" + year + "-" + month + "-" + day + "' and " +
                                    "(uid = " + uid + ") allow filtering");
                            if (resultSet2 != null) {
                                for (Row row : resultSet2) {
                                    if (result1.contains(new Integer(row.getString(0)).toString())) {
                                        results.add(new Integer(row.getString(0)).toString());
                                    }
                                }
                            }
                        }
                        mysqlConnectionStatement.close();
                        return results;
                    }
                } else if (sql.contains("Select rel.*")) {
                    String register_date = sql.substring(sql.indexOf("<") + 2, sql.indexOf("<") + 10);
                    String v = sql.substring(sql.indexOf("[") + 1, sql.indexOf("]"));
                    String[] uids = v.split(",");
                    if (uids.length != 0) {
                        StringBuilder sb = new StringBuilder();
                        sb.append("id = " + uids[0]);
                        int i = 1;
                        while (i < uids.length) {
                            sb.append(" or id = " + uids[i]);
                            i++;
                        }
                        ResultSet resultSet = mysqlConnectionStatement.executeQuery("Select * from basicinfo where (" + sb.toString() + ") and register_date < " + register_date);
                        if (resultSet != null) {
                            int count = resultSet.getMetaData().getColumnCount();
                            while (resultSet.next()) {
                                sb = new StringBuilder();
                                for (i = 1; i <= count; i++) {
                                    sb.append(resultSet.getString(i));
                                    if (i < count) {
                                        sb.append(" ");
                                    }
                                }
                                results.add(sb.toString());
                            }
                        }
                    }

                } else if (sql.equals("Select rel.id where rel.age between 20 and 30")) {
                    com.datastax.driver.core.ResultSet resultSet = cassandrasession.execute("select id from basicinfo where " +
                            "age >= 20 and age <= 30 allow filtering");
                    for (Row row : resultSet) {
                        results.add(new Integer(row.getString(0)).toString());
                    }
                }
                return results;
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (mode == 2) {
            try {
                List<String> results = new ArrayList<String>();
                if (sql.contains("select activity.device")) {
                    String start = sql.substring(sql.indexOf("between") + 8, sql.indexOf("between") + 16);
                    String end = sql.substring(sql.indexOf("between") + 21, sql.indexOf("between") + 29);
                    String v = sql.substring(sql.indexOf("[") + 1, sql.indexOf("]"));
                    String start_year = start.substring(0, 4);
                    String start_month = start.substring(4, 6);
                    String start_day = start.substring(6, 8);
                    String end_year = end.substring(0, 4);
                    String end_month = end.substring(4, 6);
                    String end_day = end.substring(6, 8);
                    String[] uids = v.split(",");
                    if (uids.length != 0) {
                        Map<String, Integer> map = new HashMap<String, Integer>();
                        for (String uid : uids) {
                            com.datastax.driver.core.ResultSet resultSet_cassandra = cassandrasession.execute("select device from activity " +
                                    "where log_time >= '" + start_year + "-" + start_month + "-" + start_day + "' " +
                                    "and log_time <= '" + end_year + "-" + end_month + "-" + end_day + "' and (uid = " +
                                    uid + ") allow filtering");
                            if (resultSet_cassandra != null) {
                                for (Row row : resultSet_cassandra) {
                                    if (!map.containsKey(row.getString(0))) {
                                        map.put(row.getString(0), 1);
                                    } else {
                                        map.put(row.getString(0), map.get(row.getString(0)).intValue() + 1);
                                    }
                                }
                            }
                        }
                        for (String s : map.keySet()) {
                            results.add(s + " " + map.get(s));
                        }
                    }
                } else if (sql.contains("Select rel.id where")) {
                    String register_date = sql.substring(sql.indexOf("<") + 2, sql.indexOf("<") + 10);
                    String log_time = sql.substring(sql.indexOf(";") - 8, sql.indexOf(";"));
                    String v = sql.substring(sql.indexOf("[") + 1, sql.indexOf("]"));
                    String[] ids = v.split(",");
                    if (ids.length != 0) {
                        String year = register_date.substring(0, 4);
                        String month = register_date.substring(4, 6);
                        String day = register_date.substring(6, 8);
                        List<String> result1 = new ArrayList<String>();
                        for (String id : ids) {
                            com.datastax.driver.core.ResultSet resultSet1 = cassandrasession.execute("select id from basicinfo where " +
                                    "register_date < '" + year + "-" + month + "-" + day + "' and (id = " + id + ") allow filtering");
                            if (resultSet1 != null) {
                                for (Row row : resultSet1) {
                                    result1.add(new Integer(row.getString(0)).toString());
                                }
                            }
                        }
                        year = log_time.substring(0, 4);
                        month = log_time.substring(4, 6);
                        day = log_time.substring(6, 8);
                        for (String uid : ids) {
                            com.datastax.driver.core.ResultSet resultSet2 = cassandrasession.execute("select uid from activity " +
                                    "where log_time = '" + year + "-" + month + "-" + day + "' and " +
                                    "(uid = " + uid + ") allow filtering");
                            if (resultSet2 != null) {
                                for (Row row : resultSet2) {
                                    if (result1.contains(new Integer(row.getString(0)).toString())) {
                                        results.add(new Integer(row.getString(0)).toString());
                                    }
                                }
                            }
                        }
                        return results;
                    }
                } else if (sql.contains("Select rel.*")) {
                    String register_date = sql.substring(sql.indexOf("<") + 2, sql.indexOf("<") + 10);
                    String v = sql.substring(sql.indexOf("[") + 1, sql.indexOf("]"));
                    String year = register_date.substring(0, 4);
                    String month = register_date.substring(4, 6);
                    String day = register_date.substring(6, 8);
                    String[] ids = v.split(",");
                    if (ids.length != 0) {
                        for (String id : ids) {
                            com.datastax.driver.core.ResultSet resultSet = cassandrasession.execute("Select * from basicinfo where (id = " + id + ") " +
                                    "and register_date < '" + year + "-" + month + "-" + day + "' allow filtering");
                            if (resultSet != null) {
                                for (Row row : resultSet) {
                                    results.add(row.getString(0) + " " + row.getString(3) + " " +
                                            row.getString(1) + " " + row.getString(2) + " " + row.getString(4)
                                            + " " + row.getDate(5).toString());
                                }
                            }
                        }
                    }
                } else if (sql.equals("Select rel.id where rel.age between 20 and 30")) {
                    com.datastax.driver.core.ResultSet resultSet = cassandrasession.execute("select id from basicinfo where " +
                            "age >= 20 and age <= 30 allow filtering");
                    for (Row row : resultSet) {
                        results.add(new Integer(row.getString(0)).toString());
                    }
                }

                return results;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    /**
     * performance test
     *
     * @return 测试性能结果
     */
    public String[] performanceTest() {
        IOInterface io = new BufferStrategy();
        String txt = io.readFile("workload/workload_default.txt");
        StringBuilder sb = new StringBuilder();
        String[] results = new String[2];
        results[0] = "The amount of user information data is 2560000, and the amount of user behavior data is 25600000.\n" +
                "The amount of query instruction is 40110, and we randomly selected 40 of them to test.\n" +
                "Intelligent method: User information data is stored in MySQL Cluster and user behavior data is stored in Cassandra.\n";
        // default方法
        long startTime1 = System.currentTimeMillis();
        try {
            Statement st = mysqlConnection.createStatement();
            String[] sqls = txt.split("\n");
            int i = 1;
            int size = sqls.length;
            double pre = 0;
            for (String sql : sqls) {
                st.execute(sql);
                if (i * 1.0 / size - pre > 0.05) {
                    pre = i * 1.0 / size;
                    System.out.println(String.format("default方法测试进度：%.2f", pre * 100) + "%");
                }
                i++;
            }
            st.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        long endTime1 = System.currentTimeMillis();
        try {
            Statement st = mysqlConnection.createStatement();
            st.execute("delete from activity where id > '102400000'");
            st.close();
            System.out.println("数据库恢复完成");
        } catch (Exception e) {
            e.printStackTrace();
        }
        long use_time1 = endTime1 - startTime1;
        sb.append("The time taken by the default method is " + use_time1 + "ms.\n");

        // 智能方法
        txt = io.readFile("workload/workload_intelligence_sql.txt");
        long startTime2 = System.currentTimeMillis();
        try {
            Statement mysqlConnectionStatement = mysqlConnection.createStatement();
            String[] sqls = txt.split("\n");
            int i = 1;
            int size = sqls.length;
            double pre = 0;
            for (String sql : sqls) {
                mysqlConnectionStatement.execute(sql);
                if (i * 1.0 / size - pre > 0.05) {
                    pre = i * 1.0 / size;
                    System.out.println(String.format("智能方法（mysql-cluster）测试进度：%.2f", pre * 100) + "%");
                }
                i++;
            }
            mysqlConnectionStatement.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        long endTime2 = System.currentTimeMillis();
        long tmp = endTime2 - startTime2;
        txt = io.readFile("workload/workload_intelligence_cql.txt");
        startTime2 = System.currentTimeMillis();
        try {
            String[] cqls = txt.split("\n");
            int i = 1;
            int size = cqls.length;
            double pre = 0;
            for (String cql : cqls) {
                cassandrasession.execute(cql);
                if (i * 1.0 / size - pre > 0.05) {
                    pre = i * 1.0 / size;
                    System.out.println(String.format("智能方法（cassandra）测试进度：%.2f", pre * 100) + "%");
                }
                i++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        endTime2 = System.currentTimeMillis();
        long use_time2 = endTime2 - startTime2 + tmp;
        sb.append("The time taken by the intelligent method is " + use_time2 + "ms.\n");

        //人工方法
        txt = io.readFile("workload/workload_artificial.txt");
        long startTime3 = System.currentTimeMillis();
        try {
            String[] cqls = txt.split("\n");
            int i = 1;
            int size = cqls.length;
            double pre = 0;
            for (String cql : cqls) {
                cassandrasession.execute(cql);
                if (i * 1.0 / size - pre > 0.05) {
                    pre = i * 1.0 / size;
                    System.out.println(String.format("人工方法测试进度：%.2f", pre * 100) + "%");
                }
                i++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        long endTime3 = System.currentTimeMillis();
        long use_time3 = endTime3 - startTime3;
        sb.append("The time taken by the artificial method is " + use_time3 + "ms.\n");
        if ((endTime3 - startTime3) * 1.0 / (endTime1 - startTime1) > 1) {
            sb.append(String.format("Default method is %.2f%% higher than Artificial method.\n", (use_time3 - use_time1) * 100.0 / (use_time3)));
        } else {
            sb.append(String.format("Artificial method is %.2f%% higher than default method.\n", (use_time1 - use_time3) * 100.0 / (use_time1)));
        }
        sb.append(String.format("Intelligent method is %.2f%% higher than default method.\n", (use_time1 - use_time2) * 100.0 / (use_time1)));
        sb.append(String.format("Intelligent method is %.2f%% higher than artificial method.\n", (use_time3 - use_time2) * 100.0 / (use_time3)));
        results[1] = sb.toString();
        return results;
    }

    /**
     * load dataset.
     *
     * @return The information of this dataset.
     */
    public String[] loadDataSet() {
        ProcessCsv p = new ProcessCsv();
        p.basicinfo_to_sql();
        p.activity_to_sql();
        long count1 = 0;
        long count2 = 0;
        try {
            Statement mysqlConnectionStatement = mysqlConnection.createStatement();
            InputStreamReader fr = null;
            BufferedReader br = null;
            try {
                fr = new InputStreamReader(new FileInputStream("basicinfo_for_sql.txt"), "UTF-8");
                br = new BufferedReader(fr);
                String rec = null;
                StringBuilder sb = new StringBuilder();
                while ((rec = br.readLine()) != null) {
                    sb.append(rec);
                    count1++;
                    if (rec.charAt(rec.length() - 1) == ';') {
                        count1--;
                        mysqlConnectionStatement.execute(sb.toString());
                        sb.delete(0, sb.length());
                    }
                }
                fr = new InputStreamReader(new FileInputStream("activity_for_sql.txt"), "UTF-8");
                br = new BufferedReader(fr);
                rec = null;
                sb = new StringBuilder();
                while ((rec = br.readLine()) != null) {
                    count2++;
                    sb.append(rec);
                    if (rec.charAt(rec.length() - 1) == ';') {
                        count2--;
                        mysqlConnectionStatement.execute(sb.toString());
                        sb.delete(0, sb.length());
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (fr != null) fr.close();
                if (br != null) br.close();
            }
            mysqlConnectionStatement.close();


            BatchStatement batch = new BatchStatement();
            com.datastax.driver.core.PreparedStatement ps = cassandrasession
                    .prepare("insert into basicinfo(id, name, age, gender, occupation, register_date) values(?,?,?,?,?,?)");
            int count = 0;
            fr = null;
            br = null;
            try {
                fr = new InputStreamReader(new FileInputStream("user_inform.csv"), "UTF-8");
                br = new BufferedReader(fr);
                String rec = null;
                StringBuilder sb = new StringBuilder();
                while ((rec = br.readLine()) != null) {
                    String tmp2[] = rec.split(",");
                    //basicinfo
                    String id = tmp2[0];
                    String name = tmp2[1];
                    int age = Integer.valueOf(tmp2[2]);
                    String gender = tmp2[3];
                    String occupation = tmp2[4];
                    String date = tmp2[5];
                    String year = date.substring(0, 4);
                    String month = date.substring(4, 6);
                    String day = date.substring(6, 8);
                    LocalDate localDate = LocalDate.fromYearMonthDay(new Integer(year).intValue(), new Integer(month).intValue(), new Integer(day).intValue());
                    BoundStatement bs = ps.bind(id, name, age, gender, occupation, localDate);
                    batch.add(bs);
                    count++;
                    if (count >= 500) {
                        cassandrasession.execute(batch);
                        batch.clear();
                        count = 0;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (fr != null) fr.close();
                if (br != null) br.close();
            }
            batch = new BatchStatement();
            ps = cassandrasession.prepare("insert into activity(id, uid, log_time, ip, device) values(?,?,?,?,?)");
            count = 0;
            fr = null;
            br = null;
            try {
                fr = new InputStreamReader(new FileInputStream("user_behavior.csv"), "UTF-8");
                br = new BufferedReader(fr);
                String rec = null;
                StringBuilder sb = new StringBuilder();
                while ((rec = br.readLine()) != null) {
                    String tmp2[] = rec.split(",");
                    String id = tmp2[0];
                    String uid = tmp2[1];
                    String date = tmp2[2];
                    String year = date.substring(0, 4);
                    String month = date.substring(4, 6);
                    String day = date.substring(6, 8);
                    LocalDate log_time = LocalDate.fromYearMonthDay(Integer.valueOf(year), Integer.valueOf(month), Integer.valueOf(day));
                    String ip = tmp2[3];
                    String device = tmp2[4];
                    BoundStatement bs = ps.bind(id, uid, log_time, ip, device);
                    batch.add(bs);
                    count++;
                    if (count >= 400) {
                        cassandrasession.execute(batch);
                        batch.clear();
                        count = 0;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (fr != null) fr.close();
                if (br != null) br.close();
            }
            String[] results = new String[3];
            results[0] = "Details:\n" +
                    "This is a relation dataset.\n" +
                    "It contains " + count1 + " user information records " +
                    "and " + count2 + " user behavior records.\n";
            results[1] = "configurations:\n" +
                    "name: user_default\n" +
                    "storage name\n" +
                    "basicinfo\n" +
                    "activity\n" +
                    "The destination databases are mysql-cluster and cassandra.\n";
            results[2] = this.toString();
            return results;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public void loadDataSet_multiThread(){
        ProcessCsv p = new ProcessCsv();
        p.basicinfo_to_sql();
        p.activity_to_sql();
        p.splitBehavior();
        ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
        cachedThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                Execution execution = new Execution();
                execution.loadDataSet_mysql();
                execution.close();
            }
        });
        cachedThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                Execution execution = new Execution();
                execution.loadDataSet_cassa_basicinfo();
                execution.close();
            }
        });
        cachedThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                Execution execution = new Execution();
                execution.loadDataSet_cassa_activity(1);
                execution.close();
            }
        });
        cachedThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                Execution execution = new Execution();
                execution.loadDataSet_cassa_activity(2);
                execution.close();
            }
        });
        cachedThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                Execution execution = new Execution();
                execution.loadDataSet_cassa_activity(3);
                execution.close();
            }
        });
        cachedThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                Execution execution = new Execution();
                execution.loadDataSet_cassa_activity(4);
                execution.close();
            }
        });
        cachedThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                Execution execution = new Execution();
                execution.loadDataSet_cassa_activity(5);
                execution.close();
            }
        });
        cachedThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                Execution execution = new Execution();
                execution.loadDataSet_cassa_activity(6);
                execution.close();
            }
        });
        cachedThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                Execution execution = new Execution();
                execution.loadDataSet_cassa_activity(7);
                execution.close();
            }
        });
        cachedThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                Execution execution = new Execution();
                execution.loadDataSet_cassa_activity(8);
                execution.close();
            }
        });
        cachedThreadPool.shutdown();
    }

    public void loadDataSet_cassa_activity(int index){
        BatchStatement batch = new BatchStatement();
        com.datastax.driver.core.PreparedStatement ps = cassandrasession.prepare("insert into activity(id, uid, log_time, ip, device) values(?,?,?,?,?)");
        int count = 0;
        InputStreamReader fr = null;
        BufferedReader br = null;
        try {
            fr = new InputStreamReader(new FileInputStream("user_behavior"+index+".csv"), "UTF-8");
            br = new BufferedReader(fr);
            String rec = null;
            StringBuilder sb = new StringBuilder();
            while ((rec = br.readLine()) != null) {
                String tmp2[] = rec.split(",");
                String id = tmp2[0];
                String uid = tmp2[1];
                String date = tmp2[2];
                String year = date.substring(0, 4);
                String month = date.substring(4, 6);
                String day = date.substring(6, 8);
                LocalDate log_time = LocalDate.fromYearMonthDay(Integer.valueOf(year), Integer.valueOf(month), Integer.valueOf(day));
                String ip = tmp2[3];
                String device = tmp2[4];
                BoundStatement bs = ps.bind(id, uid, log_time, ip, device);
                batch.add(bs);
                count++;
                if (count >= 400) {
                    cassandrasession.execute(batch);
                    batch.clear();
                    count = 0;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (fr != null) {
                try {
                    fr.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void loadDataSet_cassa_basicinfo(){
        BatchStatement batch = new BatchStatement();
        com.datastax.driver.core.PreparedStatement ps = cassandrasession
                .prepare("insert into basicinfo(id, name, age, gender, occupation, register_date) values(?,?,?,?,?,?)");
        int count = 0;
        InputStreamReader fr = null;
        BufferedReader br = null;
        try {
            fr = new InputStreamReader(new FileInputStream("user_inform.csv"), "UTF-8");
            br = new BufferedReader(fr);
            String rec = null;
            StringBuilder sb = new StringBuilder();
            while ((rec = br.readLine()) != null) {
                String tmp2[] = rec.split(",");
                //basicinfo
                String id = tmp2[0];
                String name = tmp2[1];
                int age = Integer.valueOf(tmp2[2]);
                String gender = tmp2[3];
                String occupation = tmp2[4];
                String date = tmp2[5];
                String year = date.substring(0, 4);
                String month = date.substring(4, 6);
                String day = date.substring(6, 8);
                LocalDate localDate = LocalDate.fromYearMonthDay(new Integer(year).intValue(), new Integer(month).intValue(), new Integer(day).intValue());
                BoundStatement bs = ps.bind(id, name, age, gender, occupation, localDate);
                batch.add(bs);
                count++;
                if (count >= 400) {
                    cassandrasession.execute(batch);
                    batch.clear();
                    count = 0;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (fr != null) {
                try {
                    fr.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void loadDataSet_mysql(){
        try {
            Statement mysqlConnectionStatement = mysqlConnection.createStatement();
            InputStreamReader fr = null;
            BufferedReader br = null;
            try {
                fr = new InputStreamReader(new FileInputStream("basicinfo_for_sql.txt"), "UTF-8");
                br = new BufferedReader(fr);
                String rec = null;
                StringBuilder sb = new StringBuilder();
                while ((rec = br.readLine()) != null) {
                    sb.append(rec);
                    if (rec.charAt(rec.length() - 1) == ';') {
                        mysqlConnectionStatement.execute(sb.toString());
                        sb.delete(0, sb.length());
                    }
                }
                fr = new InputStreamReader(new FileInputStream("activity_for_sql.txt"), "UTF-8");
                br = new BufferedReader(fr);
                rec = null;
                sb = new StringBuilder();
                while ((rec = br.readLine()) != null) {
                    sb.append(rec);
                    if (rec.charAt(rec.length() - 1) == ';') {
                        mysqlConnectionStatement.execute(sb.toString());
                        sb.delete(0, sb.length());
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (fr != null) fr.close();
                if (br != null) br.close();
            }
            mysqlConnectionStatement.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public String[] loadWorkload() {
        IOInterface io = new BufferStrategy();
        String txt = io.readFile("workload_default.txt");
        String[] tmp = txt.split("\n");
        Random random = new Random();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 40; i++) {
            sb.append(tmp[random.nextInt(40110) + 1] + "\n");
        }
        sb.deleteCharAt(sb.length() - 1);
        io.writeFile("workload/workload_default.txt", sb.toString());

        txt = io.readFile("workload_intelligence_sql.txt");
        tmp = txt.split("\n");
        sb = new StringBuilder();
        for (int i = 0; i < 30; i++) {
            sb.append(tmp[random.nextInt(30010) + 1] + "\n");
        }
        sb.deleteCharAt(sb.length() - 1);
        io.writeFile("workload/workload_intelligence_sql.txt", sb.toString());

        txt = io.readFile("workload_intelligence_cql.txt");
        tmp = txt.split("\n");
        sb = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            sb.append(tmp[random.nextInt(10110) + 1] + "\n");
        }
        sb.deleteCharAt(sb.length() - 1);
        io.writeFile("workload/workload_intelligence_cql.txt", sb.toString());

        txt = io.readFile("workload_artificial.txt");
        tmp = txt.split("\n");
        sb = new StringBuilder();
        for (int i = 0; i < 40; i++) {
            sb.append(tmp[random.nextInt(40120) + 1] + "\n");
        }
        sb.deleteCharAt(sb.length() - 1);
        io.writeFile("workload/workload_artificial.txt", sb.toString());
        String results[] = new String[3];
        results[0] = "Details:\n" +
                "This is a workload.\n" +
                "It contains 30 queries.\n";
        results[1] = "configurations:\n" +
                "name: workload1\n";
        results[2] = this.toString();
        return results;
    }

    public List<String> exec(String sql) {
        try {
            Statement st = mysqlConnection.createStatement();
            ResultSet resultSet = st.executeQuery(sql);
            List<String> results = new ArrayList<String>();
            if (resultSet != null) {
                int count = resultSet.getMetaData().getColumnCount();
                while (resultSet.next()) {
                    StringBuilder sb = new StringBuilder();
                    for (int i = 1; i <= count; i++) {
                        sb.append(resultSet.getString(i));
                        if (i < count) {
                            sb.append(" ");
                        }
                    }
                    results.add(sb.toString());
                }
            }
            st.close();
            return results;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        Execution execution = new Execution();
//        CostModel model = new CostModel();
//        System.out.println(model.recommend("", ""));
//        String[] results = execution.loadDataSet();
//        System.out.println(results[0]);
//        System.out.println(results[1]);
//        System.out.println(results[2]);
        execution.loadDataSet_multiThread();
//        System.out.println(execution.singleModeQuery("select activity.device, count(activity.device) from activity where log_time between 20000101 and 20201231 and uid in [101,102,103,104,105]", 1));

//        StringBuilder sb = new StringBuilder();
//        IOInterface io = new BufferStrategy();
//        for(int i = 0; i < 10; i++){
//            String[] results = execution.performanceTest();
//            sb.append(results[0] + "\n" + results[1] + "\n");
//        }
//        io.writeFile("result_50.txt", sb.toString());

//        System.out.println(execution.exec("select * from user"));
//        IOInterface io = new BufferStrategy();
//        String[] result = execution.performanceTest();
//        io.writeFile("performance_test_small.txt", result[0]+"\n"+result[1]);
        
        execution.close();
    }
}
