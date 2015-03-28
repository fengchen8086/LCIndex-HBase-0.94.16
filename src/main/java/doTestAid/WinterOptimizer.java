package doTestAid;

import java.io.IOException;

public class WinterOptimizer {

  private static boolean printOptmize = false;
  private static boolean printFunction = true;
  private static boolean prinStackForNeedImplementation = false;
  private static boolean printReplaceRawCode = false;

  public static void WaitForOptimizing(String message) {
    if (printOptmize) {
      System.out.println("winter doTest.aid.WinterOptimizer for optmize on message: " + message);
    }
  }

  public static void NeedImplementation(String message) {
    if (printFunction) {
      System.out.println("winter doTest.aid.WinterOptimizer for unhundled function on message: "
          + message);
    }
    if (prinStackForNeedImplementation) {
      new Exception(message).printStackTrace();
    }
  }

  public static void ThrowWhenCalled(String message) throws IOException {
    System.out.println("winter doTest.aid.WinterOptimizer ThrowWhenCalled on message: " + message);
    throw new IOException(message);
  }

  public static void ReplaceRawCode(String message) {
    if (printReplaceRawCode) {
      System.out.println("winter doTest.aid.WinterOptimizer replace rawcode: "
          + printReplaceRawCode);
    }
  }
}
