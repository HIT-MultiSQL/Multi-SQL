package com.relational;

/**
 *
 */


import com.relational.IOStrategy.BufferStrategy;
import com.relational.IOStrategy.IOInterface;

import java.io.*;
import java.util.Scanner;


/**
 * @author YANHAO
 *
 */
public class ProcessCsv {

    public void basicinfo_to_sql() {
        InputStreamReader fr = null;
        BufferedReader br = null;
        OutputStreamWriter fw = null;
        BufferedWriter bw = null;
        try {
            fr = new InputStreamReader(new FileInputStream("user_inform.csv"), "UTF-8");
            br = new BufferedReader(fr);
            fw = new OutputStreamWriter(new FileOutputStream("basicinfo_for_sql.txt"), "UTF-8");
            bw = new BufferedWriter(fw);
            String rec = null;
            int i = 0;
            while ((rec = br.readLine()) != null) {
                if (rec != null) {
                    if (i == 0) bw.write("insert into basicinfo values\n");
                    String tmp2[] = rec.split(",");
                    String id = tmp2[0];
                    String name = tmp2[1];
                    int age = Integer.valueOf(tmp2[2]);
                    char gender = tmp2[3].charAt(0);
                    String occupation = tmp2[4];
                    String date = tmp2[5];
                    String year = date.substring(0, 4);
                    String month = date.substring(4, 6);
                    String day = date.substring(6, 8);
                    String value =
                            "'" + id + "', '" + name + "', " + age + ", '" + gender + "', '" + occupation + "', '" + year + "" + month + "" + day + "'";
                    i++;
                    if (i == 1000) {
                        bw.write("(" + value + ");\n");
                        i = 0;
                    } else {
                        bw.write("(" + value + "),\n");
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fr.close();
                br.close();
                bw.close();
                fw.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void activity_to_sql() {
        InputStreamReader fr = null;
        BufferedReader br = null;
        OutputStreamWriter fw = null;
        BufferedWriter bw = null;
        try {
            fr = new InputStreamReader(new FileInputStream("user_behavior.csv"), "UTF-8");
            br = new BufferedReader(fr);
            fw = new OutputStreamWriter(new FileOutputStream("activity_for_sql.txt"), "UTF-8");
            bw = new BufferedWriter(fw);
            String rec = null;
            String[] argsArr = null;
            int i = 0;
            while ((rec = br.readLine()) != null) {
                if (rec != null) {
                    if (i == 0) bw.write("insert into activity values\n");
                    String[] tmp2 = rec.split(",");
                    String id = tmp2[0];
                    String uid = tmp2[1];
                    String date = tmp2[2];
                    String year = date.substring(0, 4);
                    String month = date.substring(4, 6);
                    String day = date.substring(6, 8);
                    String ip = tmp2[3];
                    String device = tmp2[4];
                    String value = "'" + id + "'" + ", '" + uid + "', '" + year + "" + month + "" + day + "', '" + ip + "', '" + device + "'";
                    i++;
                    if (i == 1000) {
                        bw.write("(" + value + ");\n");
                        i = 0;
                    } else {
                        bw.write("(" + value + "),\n");
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fr.close();
                br.close();
                bw.close();
                fw.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void splitBehavior(){
        InputStreamReader fr = null;
        BufferedReader br = null;
        OutputStreamWriter fw = null;
        BufferedWriter bw = null;
        int index = 1;
        try {
            fr = new InputStreamReader(new FileInputStream("user_behavior.csv"), "UTF-8");
            br = new BufferedReader(fr);
            while(index <= 8){
                fw = new OutputStreamWriter(new FileOutputStream("user_behavior"+index+".csv"), "UTF-8");
                bw = new BufferedWriter(fw);
                String rec = null;
                String[] argsArr = null;
                int i = 0;
                while ((rec = br.readLine()) != null) {
                    if (rec != null) {
                        i++;
                        if (i >= 160000) {
                            bw.write(rec);
                            break;
                        } else {
                            bw.write(rec+"\n");
                        }
                    }
                }
                index++;
                bw.close();
                fw.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fr.close();
                br.close();
                bw.close();
                fw.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }



    /**
     * @param args
     */
    public static void main(String[] args) {
        ProcessCsv p = new ProcessCsv();
//        p.basicinfo_to_sql();
//        p.activity_to_sql();
        p.splitBehavior();
    }

}
