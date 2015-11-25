package doTest.stream;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;

public class TestCreateLocalStream {

  static String defaultFileName = "/home/winter/temp/test.dat";

  public static void main(String[] args) throws IOException {
    new TestCreateLocalStream().work();
  }

  private static void init(ArrayList<Byte> list) {
    for (byte i = Byte.MIN_VALUE; i < Byte.MAX_VALUE; ++i) {
      list.add(i);
    }
    list.add(Byte.MAX_VALUE);
  }

  public static ArrayList<Byte> getFinalList() {
    ArrayList<Byte> list = new ArrayList<Byte>();
    ArrayList<Byte> finalList = new ArrayList<Byte>();
    init(list);
    int offset = 0, listLength = list.size();
    int range = (int) Byte.MAX_VALUE - (int) Byte.MIN_VALUE;
    while (offset <= range) {
      for (int i = 0; i < list.size(); ++i) {
        int of = (i + offset) % listLength;
        finalList.add(list.get(of));
        // System.out.println("value at: " + of + ": " + list.get(of) + "...");
      }
      offset++;
    }
    return finalList;
  }

  private static ArrayList<Byte> work() throws IOException {
    ArrayList<Byte> list = new ArrayList<Byte>();
    list = getFinalList();
    File file = new File(defaultFileName);
    FileOutputStream out = null;
    out = new FileOutputStream(file);
    for (byte b : list) {
      out.write(b);
    }
    if (out != null) {
      out.close();
    }
    System.out.println("writer file done");
    return list;
  }

  
}
