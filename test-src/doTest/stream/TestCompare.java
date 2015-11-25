package doTest.stream;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public class TestCompare {

  String strInt = "";
  String strFile = "";
  ArrayList<Short> intArray = new ArrayList<Short>();
  ArrayList<Short> fileArray = new ArrayList<Short>();
  Map<Integer, Integer> mapInt = new TreeMap<Integer, Integer>();
  Map<Integer, Integer> mapFile = new TreeMap<Integer, Integer>();

  public static void main(String[] args) throws IOException {
    String intStr = "/68/65/84/65/66/76/75/42/";
    // String intStr =
    // "/-103/-24/-122/-120/-31/-79/-74/-22/-78/-77/-27/-93/-96/-27/-111/-88/-31/-71/-102/-24/-70/-80/-22/-87";
    String fileName = "/home/winter/c00fb4e210104032b07ec6206d334af2";
    new TestCompare().work(intStr, fileName);
  }

  private void work(String targetStr, String targetFileName) throws IOException {
    initArrays(targetStr, targetFileName);
    findSub();
  }

  private void initFile(String targetFileName) throws IOException {
    InputStream is = null;
    final int BUFFER_SIZE = 1024;
    byte[] buffer = new byte[BUFFER_SIZE];
    StringBuilder sb = new StringBuilder();
    int totalCounter = 0;
    int totalLength = 0;

    try {
      is = new FileInputStream(targetFileName);
      while (true) {
        int len = is.read(buffer, 0, BUFFER_SIZE);
        if (len < 0) {
          break;
        }
        for (int i = 0; i < len; ++i) {
          short last = (short) buffer[i];
          fileArray.add(last);
          mapFile.put(totalLength, totalCounter);
          sb.append("/");
          sb.append(String.valueOf(last));
          totalLength += String.valueOf(last).length() + 1; // consider the "/"
          ++totalCounter;
        }
        // mWinterPrintBuf(buffer, 0, len, "", false, true);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (is != null) is.close();
    }
    System.out.println("out of loop");
    strFile = sb.toString();
  }

  private void initArrays(String targetStr, String targetFileName) throws IOException {
    String parts[] = targetStr.split("/");
    StringBuilder sb = new StringBuilder();
    int totalCounter = 0;
    int totalLength = 0;
    for (int i = 0; i < parts.length; ++i) {
      if (parts[i].trim().equals("")) continue;
      intArray.add(Short.valueOf(parts[i]));
      mapInt.put(totalLength, totalCounter);
      sb.append("/");
      sb.append(parts[i]);
      totalLength += parts[i].length() + 1; // consider the "/"
      ++totalCounter;
    }
    for (Entry<Integer, Integer> entry : mapInt.entrySet()) {
      System.out.println(entry.getKey() + ":" + entry.getValue());
    }
    strInt = sb.toString();

    initFile(targetFileName);
  }

  private void findSub() {
    int index = 0;
    while (true) {
      index = strFile.indexOf(strInt, index);
      if (index < 0) {
        break;
      }
      if (mapFile.containsKey(index)) {
        System.out.println("corresponding: " + mapFile.get(index));
      } else {
        System.out.println("corresponding missing for " + index);
      }
      // 16820
      // printSome(longStr, index, shortStr.length());
      index++;
    }
  }

  private void printSome(String str, int offset, int length) {
    int fix = 100;
    System.out.println(str.subSequence(offset - fix, offset));
    System.out.println(str.subSequence(offset, offset + length));
    System.out.println(str.subSequence(offset + length, offset + length + fix));
  }

  private String toCompareString(ArrayList<Short> array) {
    StringBuilder sb = new StringBuilder();
    for (Short s : array) {
      sb.append("/");
      sb.append(s);
    }
    return sb.toString();
  }

  public static void mWinterPrintBuf(byte[] buf, int offset, int length, String prefix,
      boolean showChar, boolean showInt) {
    if (!showChar && !showInt) return;
    String s1 = "", s2 = "";
    for (int i = 0; i < length && i < buf.length; ++i) {
      s1 = s1 + (char) buf[i + offset];
      s2 = s2 + "/" + (short) buf[i + offset];
    }
    if (showChar) System.out.println(prefix + ", s1 in char: " + s1);
    if (showInt) System.out.println(prefix + ", s2 in short: " + s2);
  }

}
