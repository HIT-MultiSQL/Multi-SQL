package com.hit.backup;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

public class Txtredwrt {
	/**
	 * 传入txt路径读取txt文件
	 * 
	 * @param txtPath
	 * @return 返回读取到的内容
	 */
	public static String readTxt(String txtPath) {
		File file = new File(txtPath);
		if (file.isFile() && file.exists()) {
			try {
				FileInputStream fileInputStream = new FileInputStream(file);
				InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
				BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

				StringBuffer sb = new StringBuffer();
				String text = null;
				while ((text = bufferedReader.readLine()) != null) {
					sb.append(text);
				}
				return sb.toString();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	/**
	 * 使用FileOutputStream来写入txt文件
	 * 
	 * @param txtPath txt文件路径
	 * @param content 需要写入的文本
	 */
	public static void writeTxt(String file, String txt) {
		File fileDemo1 = new File(file);
		// System.out.println(fileDemo1.getParentFile());
		if (!(fileDemo1.getParentFile().exists())) {
			fileDemo1.getParentFile().mkdirs();
		}
		if (!fileDemo1.exists()) { // 如果存在这个文件就删除，否则就创建
			try {
				System.out.println(fileDemo1.createNewFile());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		BufferedWriter out = null;
		try {
			out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true)));
			out.write(txt + "\r\n");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
