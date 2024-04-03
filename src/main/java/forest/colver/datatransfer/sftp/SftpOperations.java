package forest.colver.datatransfer.sftp;

import static forest.colver.datatransfer.sftp.Utils.*;
import static forest.colver.datatransfer.sftp.Utils.connectChannelSftp;
import static forest.colver.datatransfer.sftp.Utils.getKeySession;
import static forest.colver.datatransfer.sftp.Utils.getPwSession;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
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
   * Places a file on an SFTP server. This method builds a session and channel using password auth,
   * for the one connection. If possible, use the {@link #putSftpFile putSftpFile} method that takes
   * an sftp channel argument.
   */
  public static void putSftpFileUsePasswordAndSession(String host, String username, String password,
      String path,
      String filename, String payload)
      throws JSchException, SftpException, IOException {
    var session = getPwSession(host, username, password);
    var sftp = connectChannelSftp(session);
    putSftpFile(sftp, path, filename, payload);
    sftp.exit();
    session.disconnect();
    LOG.info(SFTP_EXIT);
  }

  /**
   * Retrieves and then deletes a file from the SFTP server. This method builds a session and
   * channel using password auth, for the one connection. If possible, use the
   * {@link #consumeSftpFile consumeSftpFile} method that takes an sftp channel argument.
   */
  public static String consumeSftpFileUsePasswordAndSession(String host, String username,
      String password,
      String path,
      String filename) throws JSchException, SftpException, IOException {
    var session = getPwSession(host, username, password);
    var sftp = connectChannelSftp(session);
    var contents = consumeSftpFile(sftp, path, filename);
    sftp.exit();
    session.disconnect();
    LOG.info(SFTP_EXIT);
    return contents;
  }

  /**
   * Places a file on an SFTP server. This method builds a session and channel using key auth, for
   * the one connection. If possible, use the {@link #putSftpFile putSftpFile} method that takes an
   * sftp channel argument.
   */
  public static void putSftpFileUseKeyAndSession(String host, String username, String keyLocation,
      String path,
      String filename, String payload)
      throws JSchException, SftpException, IOException {
    var session = getKeySession(host, username, keyLocation);
    var sftp = connectChannelSftp(session);
    putSftpFile(sftp, path, filename, payload);
    sftp.exit();
    session.disconnect();
    LOG.info(SFTP_EXIT);
  }

  /**
   * Retrieves and then deletes a file from the SFTP server. This method builds a session and
   * channel using key auth, for the one connection. If possible, use the
   * {@link #consumeSftpFile consumeSftpFile} method that takes an sftp channel argument.
   */
  public static String consumeSftpFileUseKeyAndSession(String host, String username,
      String keyLocation,
      String path,
      String filename) throws JSchException, SftpException, IOException {
    var session = getKeySession(host, username, keyLocation);
    var sftp = connectChannelSftp(session);
    var contents = consumeSftpFile(sftp, path, filename);
    sftp.exit();
    session.disconnect();
    LOG.info(SFTP_EXIT);
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

  // todo: probably need a sftpDeleteFile method

}
