package forest.colver.datatransfer.config;

import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * General utils
 */
public class Utils {

  // todo: move all tech-specific utils to their package (AWS or Azure or qpid). Leave general utils
  // todo: also, I think some of these methods are not used. Either remove them or write Javadocs so it's easier to remember what they are for
  public static final String TIME_STAMP =
      new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss.SSS").format(new Date());
  static final long DEFAULT_TTL = 60_000L; // time to live in milliseconds
  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);
  public static String defaultPayload = "Default Payload: " + TIME_STAMP;
  // user cred file
  static final String CREDS_DEFAULT_PATH = "/.emx/credentials.properties";
  public static Properties userCreds = getCredentials();
  static String password = userCreds.getProperty("emx.password");
  static String username = userCreds.getProperty("emx.username");
  // get Qpid endpoints
  public static final String QPID_DEV = userCreds.getProperty("qpid.dev");
  public static final String QPID_TEST = userCreds.getProperty("qpid.test");
  public static final String QPID_STAGE = userCreds.getProperty("qpid.stage");
  public static final String QPID_PROD = userCreds.getProperty("qpid.prod");
  // get Task Queues endpoints
  public static final String TQ_STAGE = userCreds.getProperty("tq.stage");
  public static final String TQ_PROD = userCreds.getProperty("tq.prod");

  public static String readFile(String path, Charset encoding) {
    byte[] encoded = new byte[0];
    try {
      encoded = Files.readAllBytes(Paths.get(path));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return new String(encoded, encoding);
  }

  public static void writeFile(String fullyQualifiedFilePath, byte[] contents) {
    var path = Paths.get(fullyQualifiedFilePath);
    try {
      Files.write(path, contents);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static Properties getCredentials(String path) {
    Properties credentials = new Properties();
    try {
      FileReader reader = new FileReader(path);
      credentials.load(reader);
    } catch (Exception var3) {
      System.out.println(
          "File not found from path given OR properties in file are not properly formatted (ex. emx.username=myuser)\nDefault path is %HOME%/.emx/credentials.properties");
    }
    return credentials;
  }

  public static Properties getCredentials() {
    String homeDirectory = System.getProperty("user.home");
    String fullPath = homeDirectory + CREDS_DEFAULT_PATH;
    return getCredentials(fullPath);
  }

  public static String getDefaultPayload() {
    return defaultPayload;
  }

  public static String getTimeStamp() {
    return TIME_STAMP;
  }

  public static String getUsername() {
    return username;
  }

  public static String getPassword() {
    return password;
  }

  public static void sleepo(int millis) {
    LOG.info("Sleeping for {} seconds...", millis / 1000);
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
