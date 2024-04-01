package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.sftp.SftpOperations.consumeSftpFile;
import static forest.colver.datatransfer.sftp.SftpOperations.consumeSftpFilePasswordAndSession;
import static forest.colver.datatransfer.sftp.SftpOperations.putSftpFile;
import static forest.colver.datatransfer.sftp.SftpOperations.putSftpFilePasswordAndSession;
import static forest.colver.datatransfer.sftp.Utils.SFTP_HOST;
import static forest.colver.datatransfer.sftp.Utils.SFTP_KEY;
import static forest.colver.datatransfer.sftp.Utils.SFTP_PASSWORD;
import static forest.colver.datatransfer.sftp.Utils.connectChannelSftp;
import static forest.colver.datatransfer.sftp.Utils.getKeySession;
import static forest.colver.datatransfer.sftp.Utils.getPwSession;
import static forest.colver.datatransfer.sftp.Utils.sftpDisconnect;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import java.io.IOException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SftpIntTests {

  static final Logger LOG = LoggerFactory.getLogger(SftpIntTests.class);
  public static final String USERNAME = "revloc02";
  public static final String PATH = "target/test";
  public static final String FILENAME = "sftp-test.txt";
  public static final String PAYLOAD = "payload";
  public static final String SFTP_KEY_LOC = "src/main/resources/prvKey";

  static Session SESSION_PW;
  static ChannelSftp SFTP_CH_PW;
  static Session SESSION_KEY;
  static ChannelSftp SFTP_CH_KEY;

  /**
   * Creates an SFTP channel for both a password auth and a key auth to be used by any of the
   * following unit tests.
   */
  @BeforeAll
  static void setupSftp() throws JSchException {
    SESSION_PW = getPwSession(SFTP_HOST, USERNAME, SFTP_PASSWORD);
    SFTP_CH_PW = connectChannelSftp(SESSION_PW);
    SESSION_KEY = getKeySession(SFTP_HOST, USERNAME, SFTP_KEY_LOC);
    SFTP_CH_KEY = connectChannelSftp(SESSION_KEY);
  }

  @AfterAll
  static void cleanupSftp() {
    sftpDisconnect(SFTP_CH_PW, SESSION_PW);
    sftpDisconnect(SFTP_CH_KEY, SESSION_KEY);
  }

  @Test
  void testSftpPut() throws SftpException, IOException {
    putSftpFile(SFTP_CH_PW, PATH, FILENAME, PAYLOAD);
    var contents = consumeSftpFile(SFTP_CH_PW, PATH, FILENAME);
    assertThat(contents).isEqualTo(PAYLOAD);
  }

  @Test
  void testSftpConsume() throws SftpException, IOException {
    putSftpFile(SFTP_CH_PW, PATH, FILENAME, PAYLOAD);
    var contents = consumeSftpFile(SFTP_CH_PW, PATH, FILENAME);
    assertThat(contents).isEqualTo(PAYLOAD);
  }

  @Test
  void testAwsSftpAuthFails() throws SftpException, IOException, InterruptedException {
    for (var i = 0; i < 5; i++) {

      // do a successful password auth
      putSftpFile(SFTP_CH_PW, PATH, FILENAME, PAYLOAD);
      consumeSftpFile(SFTP_CH_PW, PATH, FILENAME);

      // do a successful key auth
      putSftpFile(SFTP_CH_KEY, PATH, FILENAME, PAYLOAD);
      consumeSftpFile(SFTP_CH_KEY, PATH, FILENAME);

      var username = "hercules"; // this account does no exist
      // do an unsuccessful password auth
      assertThatExceptionOfType(JSchException.class).isThrownBy(
          () -> getPwSession(SFTP_HOST, username, "bogus_password"));

      // do an unsuccessful key auth
      assertThatExceptionOfType(JSchException.class).isThrownBy(
          () -> getKeySession(SFTP_HOST, username, "src/main/resources/badPrvKey"));

      Thread.sleep(5000);
    }
  }

  /**
   * This tests the SFTP operations that create a session for the one operation.
   */
  @Test
  void testPutAndConsumeSftpFilePasswordAndSession()
      throws JSchException, SftpException, IOException {
    putSftpFilePasswordAndSession(SFTP_HOST, USERNAME, SFTP_PASSWORD, PATH, FILENAME, PAYLOAD);
    var contents = consumeSftpFilePasswordAndSession(SFTP_HOST, USERNAME, SFTP_PASSWORD, PATH,
        FILENAME);
    assertThat(contents).isEqualTo(PAYLOAD);
  }
}
