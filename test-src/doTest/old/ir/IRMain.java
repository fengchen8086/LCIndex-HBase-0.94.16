package doTest.old.ir;

public class IRMain {
  public static void main(String args[]) throws Exception{
    // create
    new IRCreateTable().work();
    // put data and flush
    new IRPutData().work(true);
    System.out.println("IRMain finish");
  }
}
