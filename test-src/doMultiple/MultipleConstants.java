package doMultiple;

public class MultipleConstants {
  public static final String FILE_NAME_FORMAT = "%06d";

  public static String getFileName(String prefix, int id) {
    return prefix + "-" + String.format(FILE_NAME_FORMAT, id);
  }
}
