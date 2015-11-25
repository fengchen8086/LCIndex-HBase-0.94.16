package aid;


public class ValueParser {

  public void parseDouble(){
    double d = 005.23;
    String str = Double.toString(d);
    System.out.println(str);
    String s = "005.321";
    double aDouble = Double.parseDouble(s);
    System.out.println(aDouble);
  }
  
  public static void main(String[] args) {
    new ValueParser().parseString();
  }

  private void parseString() {
    int i = 5;
    String str = String.format("%05d", i);
    System.out.println(str);
  }
}
