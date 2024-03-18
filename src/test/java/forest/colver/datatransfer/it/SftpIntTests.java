package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.sftp.SftpOperations.consumeSftpFile;
import static forest.colver.datatransfer.sftp.SftpOperations.putSftpFile;
import static forest.colver.datatransfer.sftp.Utils.SFTP_HOST;
import static forest.colver.datatransfer.sftp.Utils.SFTP_KEY;
import static forest.colver.datatransfer.sftp.Utils.SFTP_PASSWORD;
import static forest.colver.datatransfer.sftp.Utils.connectChannelSftp;
import static forest.colver.datatransfer.sftp.Utils.getKeySession;
import static forest.colver.datatransfer.sftp.Utils.getPwSession;
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

  static Session SESSION_PW;
  static ChannelSftp SFTP_CH_PW;
  static Session SESSION_KEY;
  static ChannelSftp SFTP_CH_KEY;

  @BeforeAll
  static void setupSftp() throws JSchException {
    SESSION_PW = getPwSession(SFTP_HOST, USERNAME, SFTP_PASSWORD);
    SFTP_CH_PW = connectChannelSftp(SESSION_PW);
    SESSION_KEY = getKeySession(SFTP_HOST, USERNAME, SFTP_KEY);
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

  // TODO: ok, at this point, now that I have some good code in place, I need to revisit why I an setting this up and make stuff to allow me to do that thing.
}
