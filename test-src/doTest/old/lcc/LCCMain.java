package doTest.old.lcc;

public class LCCMain {
  public static void usage() {
    System.out.println("LCCMain recordNumber(int) forceflush(true/false) resourcePath(may be blank)");
  }

  public static void main(String args[]) throws Exception {
    int recordNumber = 0;
    boolean forceFlush = false;
    String resourcePath = null;
    usage();

//    if (args.length >= 1) recordNumber = Integer.valueOf(args[0]);
//    if (args.length >= 2) forceFlush = Boolean.valueOf(args[1]);
//    if (args.length >= 3) resourcePath = args[2];

    recordNumber = 1000;
    forceFlush = true;
    resourcePath = null;
    
    System.out.println("set record number: " + recordNumber);
    System.out.println("set forceflush: " + forceFlush);
    System.out.println("set resource path: " + resourcePath);

    // create
    new LCCCreateIndexTable().work(resourcePath);
    // // put data and flush
    new LCCPutData(resourcePath).work(recordNumber, forceFlush);
    System.out.println("LCCMain finish");
  }
}
