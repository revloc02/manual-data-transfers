package forest.colver.datatransfer.config;

import static java.util.UUID.randomUUID;

import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * General utils
 */
public class Utils {

  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

  public static final String TIME_STAMP =
      new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss.SSS").format(new Date());
  public static String defaultPayload = "Default Payload: " + TIME_STAMP;

  static final long DEFAULT_TTL = 60_000L; // time to live in milliseconds
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
  public static final String QPID_STAGE_HOST = userCreds.getProperty("qpid.stage.host");
  public static final String QPID_PROD_HOST = userCreds.getProperty("qpid.prod.host");
  // get Task Queues endpoints
  public static final String TQ_STAGE = userCreds.getProperty("tq.stage");
  public static final String TQ_PROD = userCreds.getProperty("tq.prod");

  /**
   * Used to read a file from disk, usually used for a payload of a message.
   *
   * @param path The path to the file, e.g. "src/test/resources/1test.txt"
   * @param encoding The file encoding, typically StandardCharsets.UTF_8.
   * @return The file's contents.
   */
  public static String readFile(String path, Charset encoding) {
    byte[] encoded = new byte[0];
    try {
      encoded = Files.readAllBytes(Paths.get(path));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return new String(encoded, encoding);
  }

  /**
   * Used to write a file to disk, most useful when a message payload is too big to peruse in a
   * log.
   *
   * @param fullyQualifiedFilePath The path to the intended location of the written file.
   * @param contents The contents to be written to the file.
   */
  public static void writeFile(String fullyQualifiedFilePath, byte[] contents) {
    var path = Paths.get(fullyQualifiedFilePath);
    try {
      Files.write(path, contents);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Generates a list of uuids, currently used for unique message payloads.
   *
   * @param num The number of uuids to generate.
   * @return A list of uuid strings.
   */
  public static ArrayList<String> generateUniqueStrings(int num) {
    var uuids = new ArrayList<String>();
    for (int i = 0; i < num; i++) {
      var uuid = randomUUID().toString();
      uuids.add(uuid);
    }
    return uuids;
  }

  /**
   * Gets credentials from local credentials file.
   *
   * @param path The fully qualified location of the credentials file.
   * @return List of credential Properties from the file.
   */
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

  /**
   * Builds the default path for credentials retrieval.
   *
   * @return List of credential Properties from the file.
   */
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

  public static void pause(int seconds) {
    LOG.info("Pausing for {} seconds...", seconds);
    try {
      Thread.sleep(seconds * 1_000L);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
