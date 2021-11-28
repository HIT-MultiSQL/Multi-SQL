/**
 * 
 */
package com.hit.backup.optimization.IOStrategy;

import java.io.File;

/**
 * This is io strategy interface.
 * @author YANHAO
 *
 */
public interface IOInterface {

  public String readFile(String fileName);

  public void writeFile(String fileName, String txt);

}
