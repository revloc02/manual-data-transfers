package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.sftp.SftpOperations.consumeSftpFile;
import static forest.colver.datatransfer.sftp.SftpOperations.consumeSftpFileUseKeyManageSession;
import static forest.colver.datatransfer.sftp.SftpOperations.consumeSftpFileUsePasswordManageSession;
import static forest.colver.datatransfer.sftp.SftpOperations.putSftpFile;
import static forest.colver.datatransfer.sftp.SftpOperations.putSftpFileUseKeyManageSession;
import static forest.colver.datatransfer.sftp.SftpOperations.putSftpFileUsePasswordManageSession;
import static forest.colver.datatransfer.sftp.Utils.SFTP_HOST;
import static forest.colver.datatransfer.sftp.Utils.SFTP_PASSWORD;
import static forest.colver.datatransfer.sftp.Utils.connectChannelSftp;
import static forest.colver.datatransfer.sftp.Utils.getKeySession;
import static forest.colver.datatransfer.sftp.Utils.getPwSession;
import static forest.colver.datatransfer.sftp.Utils.sftpDisconnect;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import java.io.IOException;
import java.time.Instant;
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

  @Test
  void testSftpPutConsume_Pw_ManageChannel() throws SftpException, IOException, JSchException {
    var session = getPwSession(SFTP_HOST, USERNAME, SFTP_PASSWORD);
    var sftp = connectChannelSftp(session);
    putSftpFile(sftp, PATH, FILENAME, PAYLOAD);
    var contents = consumeSftpFile(sftp, PATH, FILENAME);
    assertThat(contents).isEqualTo(PAYLOAD);
  }

  @Test
  void testSftpPutConsume_Key_ManageChannel() throws SftpException, IOException, JSchException {
    var session = getKeySession(SFTP_HOST, USERNAME, SFTP_KEY_LOC);
    var sftp = connectChannelSftp(session);
    putSftpFile(sftp, PATH, FILENAME, PAYLOAD);
    var contents = consumeSftpFile(sftp, PATH, FILENAME);
    assertThat(contents).isEqualTo(PAYLOAD);
    sftpDisconnect(sftp, session);
  }

  /** Runs a series of AWS SFTP authentication tests, both successes and failures. */
  @Test
  void testAwsSftpAuthFails()
      throws SftpException, IOException, InterruptedException, JSchException {
    for (var i = 0; i < 3; i++) {

      LOG.info("...non-existent account password auth...");
      assertThatExceptionOfType(JSchException.class)
          .isThrownBy(() -> getPwSession(SFTP_HOST, "unknown-pw-user", "moot"));

      LOG.info("...non-existent account key auth...");
      assertThatExceptionOfType(JSchException.class)
          .isThrownBy(
              () -> getKeySession(SFTP_HOST, "unknown-key-user", "src/main/resources/badPrvKey"));

      LOG.info("...do an unsuccessful password auth...");
      assertThatExceptionOfType(JSchException.class)
          .isThrownBy(
              () -> {
                var session = getPwSession(SFTP_HOST, "emx-app-test", "bogus_password");
                var sftp = connectChannelSftp(session);
                putSftpFile(sftp, PATH, "this-should-not-work.txt", PAYLOAD);
              });

      LOG.info("...do an unsuccessful key auth...");
      assertThatExceptionOfType(JSchException.class)
          .isThrownBy(
              () -> {
                var session =
                    getKeySession(
                        SFTP_HOST, "emx-app-test-keyauth", "src/main/resources/badPrvKey");
                var sftp = connectChannelSftp(session);
                putSftpFile(sftp, PATH, "this-should-not-work.txt", PAYLOAD);
              });

      LOG.info("...do a successful password auth...");
      putSftpFileUsePasswordManageSession(
          SFTP_HOST, "revloc02a", SFTP_PASSWORD, PATH, FILENAME, PAYLOAD);
      var contents =
          consumeSftpFileUsePasswordManageSession(
              SFTP_HOST, "revloc02a", SFTP_PASSWORD, PATH, FILENAME);
      assertThat(contents).isEqualTo(PAYLOAD);

      LOG.info("...do a successful key auth...");
      putSftpFileUseKeyManageSession(SFTP_HOST, "revloc02", SFTP_KEY_LOC, PATH, FILENAME, PAYLOAD);
      contents =
          consumeSftpFileUseKeyManageSession(SFTP_HOST, "revloc02", SFTP_KEY_LOC, PATH, FILENAME);
      assertThat(contents).isEqualTo(PAYLOAD);

      LOG.info("========== Done with iteration: {} TIME: {} ==========\n", i, Instant.now());
    }
  }

  /**
   * This tests the SFTP operations that create a session using password auth, for the one
   * operation.
   */
  @Test
  void testSftpPutConsume_PasswordMakeSession() throws JSchException, SftpException, IOException {
    putSftpFileUsePasswordManageSession(
        SFTP_HOST, USERNAME, SFTP_PASSWORD, PATH, FILENAME, PAYLOAD);
    var contents =
        consumeSftpFileUsePasswordManageSession(SFTP_HOST, USERNAME, SFTP_PASSWORD, PATH, FILENAME);
    assertThat(contents).isEqualTo(PAYLOAD);
  }

  /** This tests the SFTP operations that create a session using key auth, for the one operation. */
  @Test
  void testSftpPutConsume_UseKeyMakeSession() throws JSchException, SftpException, IOException {
    putSftpFileUseKeyManageSession(SFTP_HOST, USERNAME, SFTP_KEY_LOC, PATH, FILENAME, PAYLOAD);
    var contents =
        consumeSftpFileUseKeyManageSession(SFTP_HOST, USERNAME, SFTP_KEY_LOC, PATH, FILENAME);
    assertThat(contents).isEqualTo(PAYLOAD);
  }
}
