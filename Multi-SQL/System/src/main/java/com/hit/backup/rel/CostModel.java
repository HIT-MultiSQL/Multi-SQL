package com.hit.backup.rel;

import com.hit.backup.rel.IOStrategy.BufferStrategy;
import com.hit.backup.rel.IOStrategy.IOInterface;

import java.io.*;

public class CostModel {

    /**
     * The recommended results are obtained according to the cost model
     * @return recommend result
     */
    public String recommend(String information_for_loadDateset, String information_for_loadWorkload){
        StringBuilder sb = new StringBuilder();
        Process proc;
        try {
            proc = Runtime.getRuntime().exec("python ../storePredictModule/main.py");// 执行py文件
            //用输入输出流来截取结果
            BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            String line = null;
            while ((line = in.readLine()) != null) {
                sb.append(line + "\n");
            }
            in.close();
            proc.waitFor();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        CostModel model = new CostModel();
    }
}
