public final class Config
{
  public static String curDir = "..";

  public static String configDir = "../etc";
  public static String logDir = "../log";

  public static void config(String configDir, String logDir, String curDir)
  {
    curDir = curDir;
    configDir = configDir;
    logDir = logDir;
  }

  public static void config(String configDir, String logDir)
  {
    configDir = configDir;
    logDir = logDir;
  }

  public static String getConfigDir()
  {
    return configDir;
  }

  public static String getLogDir()
  {
    return logDir;
  }
}