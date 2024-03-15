package forest.colver.datatransfer.sftp;

import static forest.colver.datatransfer.sftp.Utils.connectChannelSftp;
import static forest.colver.datatransfer.sftp.Utils.getPwSession;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SftpOperations {

  static final Logger LOG = LoggerFactory.getLogger(SftpOperations.class);

  private SftpOperations() {
    // https://rules.sonarsource.com/java/RSPEC-1118/
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated.");
  }

  /**
   * Places a file on an SFTP server. This method builds a session and channel for the one
   * connection. If possible, use the method that has an sftp channel argument.
   */
  public static void putSftpFilePassword(String host, String username, String password, String path,
      String filename, String payload)
      throws Throwable {
    var session = getPwSession(host, username, password);
    var sftp = connectChannelSftp(session);
    try {
      sftp.put(new ByteArrayInputStream(payload.getBytes()), path + "/" + filename);
    } finally {
      sftp.exit();
      session.disconnect();
      LOG.info("sftp exit");
    }
  }

  /**
   * Retrieves and then deletes a file from the SFTP server. This method builds a session and
   * channel for the one connection. If possible, use the method that has an sftp channel argument.
   */
  public static String consumeSftpFilePassword(String host, String username, String password,
      String path,
      String filename) throws Throwable {
    var session = getPwSession(host, username, password);
    var sftp = connectChannelSftp(session);
    String contents;
    try (var inputStream = sftp.get(path + "/" + filename)) {
      contents = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
      LOG.info("sftp retrieve done");
      sftp.rm(path + "/" + filename);
    } finally {
      sftp.exit();
      session.disconnect();
      LOG.info("sftp exit");
    }
    return contents;
  }

  /**
   * Places a files on the SFTP server.
   *
   * @param sftp An SFTP Channel (assumes a session and channel was set up using proper credentials
   * and passed in).
   * @param path The path the file.
   * @param filename The name of the file.
   * @param payload The contents of the file.
   */
  public static void putSftpFile(ChannelSftp sftp, String path, String filename, String payload)
      throws SftpException, IOException {
    try (var file = new ByteArrayInputStream(payload.getBytes())) {
      sftp.put(file, path + "/" + filename);
    }
  }

  /**
   * Retrieves and then deletes a file from the SFTP server.
   *
   * @param sftp An SFTP Channel (assumes a session and channel was set up using proper credentials
   * and passed in).
   * @param path The path to the file.
   * @param filename The name of the file.
   * @return The contents of the file--the payload.
   */
  public static String consumeSftpFile(ChannelSftp sftp, String path, String filename)
      throws IOException, SftpException {
    String contents;
    try (var inputStream = sftp.get(path + "/" + filename)) {
      contents = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
      LOG.info("sftp retrieve done");
      sftp.rm(path + "/" + filename);
    }
    return contents;
  }

}
