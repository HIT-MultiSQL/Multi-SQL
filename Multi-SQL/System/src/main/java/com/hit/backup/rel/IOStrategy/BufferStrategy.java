/**
 * 
 */
package com.hit.backup.rel.IOStrategy;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

/**
 * This is buffer io strategy.
 * 
 * @author YANHAO
 * 
 */
public class BufferStrategy implements IOInterface {

  /*
   * (non-Javadoc)
   * 
   * @see IOStrategy.IOInterface#readFile(java.lang.String)
   */
  @Override
  public String readFile(String fileName) {
    BufferedReader br;
    StringBuilder sb = new StringBuilder();
    try {
      br = new BufferedReader(new FileReader(new File(fileName)));
      String s;
      while ((s = br.readLine()) != null) {
        sb.append(s + "\n");
      }
      br.close();
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return sb.toString();
  }

  /*
   * (non-Javadoc)
   * 
   * @see IOStrategy.IOInterface#writeFile(java.io.File)
   */
  @Override
  public void writeFile(String fileName, String txt) {
    try {
      BufferedWriter bw = new BufferedWriter(new FileWriter(new File(fileName)));
      bw.write(txt);
      bw.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

}
