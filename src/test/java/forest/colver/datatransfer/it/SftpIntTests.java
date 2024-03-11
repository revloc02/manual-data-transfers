package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.sftp.SftpOperations.consumeSftpFile;
import static forest.colver.datatransfer.sftp.SftpOperations.consumeSftpFilePassword;
import static forest.colver.datatransfer.sftp.SftpOperations.putSftpFile;
import static forest.colver.datatransfer.sftp.SftpOperations.putSftpFilePassword;
import static forest.colver.datatransfer.sftp.Utils.SFTP_HOST;
import static forest.colver.datatransfer.sftp.Utils.SFTP_PASSWORD;
import static forest.colver.datatransfer.sftp.Utils.connectChannelSftp;
import static forest.colver.datatransfer.sftp.Utils.getSession;
import static forest.colver.datatransfer.sftp.Utils.sftpDisconnect;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

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
  static Session SESSION;
  static ChannelSftp SFTP;

  @BeforeAll
  static void setupSftp() throws JSchException {
    SESSION = getSession(SFTP_HOST, USERNAME, SFTP_PASSWORD);
    SFTP = connectChannelSftp(SESSION);
  }

  @AfterAll
  static void cleanupSftp() {
    sftpDisconnect(SFTP, SESSION);
  }

  @Test
  void testSftpPut() throws Throwable {
    // todo: I should probably? pass the sftp-channel in each test so that it doesn't have to open on for each operation
    putSftpFilePassword(SFTP_HOST, USERNAME, SFTP_PASSWORD, PATH, FILENAME, PAYLOAD);
    var contents = consumeSftpFilePassword(SFTP_HOST, USERNAME, SFTP_PASSWORD, PATH, FILENAME);
    assertThat(contents).isEqualTo(PAYLOAD);
  }

  @Test
  void testSftpPutPassChannel() throws SftpException, IOException {
    putSftpFile(SFTP, PATH, FILENAME, PAYLOAD);
    var contents = consumeSftpFile(SFTP, PATH, FILENAME);
    assertThat(contents).isEqualTo(PAYLOAD);
  }

  // TODO: ok, at this point, now that I have some good code in place, I need to revisit why I an setting this up and make stuff to allow me to do that thing.
}
