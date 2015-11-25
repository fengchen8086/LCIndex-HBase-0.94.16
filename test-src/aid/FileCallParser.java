package aid;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class FileCallParser {

  static class Pair {
    String a;
    String b;

    public Pair(String m, String n) {
      if (m.compareTo(n) < 0) {
        a = m;
        b = n;
      } else {
        a = n;
        b = m;
      }
    }

    public static boolean isSame(Pair x, Pair y) {
      return x.a.equals(y.a) && x.b.equals(y.b);
    }
  }

  public static void main(String[] args) throws IOException {
    String flush = "winter flushing memstore to file";
    String minorCompact = "winter compact minor for file";
    String commit = "winter renaming flushed lccindex local";
    String completeCompaction = "winter renaming compacted local index file";
    Set<String> flushSet = new TreeSet<String>();
    Set<String> minorCompactSet = new TreeSet<String>();
    Set<String> commitFromSet = new TreeSet<String>();
    Set<String> commitToSet = new TreeSet<String>();
    Set<String> completeCompactionFromSet = new TreeSet<String>();
    Set<String> completeCompactionToSet = new TreeSet<String>();

    String fileName = "/home/winter/user-cmd/temp/here";
    BufferedReader br = new BufferedReader(new FileReader(fileName));
    String line = null;
    while ((line = br.readLine()) != null) {
      if (line.startsWith(flush)) {
        flushSet.add(updateFlush(line));
      } else if (line.startsWith(minorCompact)) {
        minorCompactSet.add(updateMinorCompact(line));
      } else if (line.startsWith(commit)) {
        updateCommit(line, commitFromSet, commitToSet);
      } else if (line.startsWith(completeCompaction)) {
        updateCompleteCompactionSet(line, completeCompactionFromSet, completeCompactionToSet);
      }
    }
    br.close();

    Map<String, Set<String>> map = new TreeMap<String, Set<String>>();
    map.put("flush", flushSet);
    map.put("minorCompact", minorCompactSet);
    map.put("commitFrom", commitFromSet);
    map.put("commitTo", commitToSet);
    map.put("completeCompactionFrom", completeCompactionFromSet);
    map.put("completeCompactionTo", completeCompactionToSet);
    compareSets(map);
  }

  public static void compareSets(Map<String, Set<String>> map) {
    List<Pair> pairList = new ArrayList<Pair>();
    for (Entry<String, Set<String>> e1 : map.entrySet()) {
      for (Entry<String, Set<String>> e2 : map.entrySet()) {
        if (e1 == e2) {
          continue;
        }
        if (updatePair(pairList, new Pair(e1.getKey(), e2.getKey()))) {
          if (compareTwoSet(e1.getKey(), e1.getValue(), e2.getKey(), e2.getValue())) {
            System.out.println(e1.getKey() + " and " + e2.getKey() + "have same value");
          }
        }
      }
    }
  }

  private static boolean compareTwoSet(String key, Set<String> s1, String key2, Set<String> s2) {
    for (String s : s1) {
      if (s2.contains(s)) {
        return true;
        // System.out.println("both " + key + " and " + key2 + " contains " + s);
      }
    }
    return false;
  }

  private static boolean updatePair(List<Pair> pairList, Pair newPair) {
    for (Pair p : pairList) {
      if (Pair.isSame(p, newPair)) {
        return true;
      }
    }
    pairList.add(newPair);
    return false;
  }

  public static String updateFlush(String line) {
    String prefix = "winter flushing memstore to file: /home/winter/temp/data/lccindex/tpch_lcc/";
    return line.substring(prefix.length());
  }

  public static String updateMinorCompact(String line) {
    String prefix = "winter compact minor for file: file:/home/winter/temp/data/lccindex/tpch_lcc/";
    return line.substring(prefix.length());
  }

  public static void updateCommit(String line, Set<String> commitFromSet, Set<String> commitToSet) {
    String prefix = "winter renaming flushed lccindex local file from ";
    String[] parts = line.substring(prefix.length()).split(" to ");
    String fromPrefix = "file:/home/winter/temp/data/lccindex/tpch_lcc/";
    String toPrefix = "/home/winter/temp/data/lccindex/tpch_lcc/";
    commitFromSet.add(parts[0].substring(fromPrefix.length()));
    commitToSet.add(parts[1].substring(toPrefix.length()));
  }

  private static void updateCompleteCompactionSet(String line,
      Set<String> completeCompactionFromSet, Set<String> completeCompactionToSet) {
    String prefix = "winter renaming compacted local index file at ";
    String[] parts = line.substring(prefix.length()).split(" to ");
    String fromPrefix = "file:/home/winter/temp/data/lccindex/tpch_lcc/";
    String toPrefix = "/home/winter/temp/data/lccindex/tpch_lcc/";
    completeCompactionFromSet.add(parts[0].substring(fromPrefix.length()));
    completeCompactionToSet.add(parts[1].substring(toPrefix.length()));
  }
}
