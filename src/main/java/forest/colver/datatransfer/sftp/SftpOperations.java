package forest.colver.datatransfer.sftp;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.Session;
import java.io.ByteArrayInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SftpOperations {
  static final Logger LOG = LoggerFactory.getLogger(SftpOperations.class);

  private SftpOperations() {
    // https://rules.sonarsource.com/java/RSPEC-1118/
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated.");
  }

  public static void putSftpFilePassword(
      String host, String username, String password, String filename, String path, String payload)
      throws Throwable {

    Session session = Utils.getSession(username, password, host);
    ChannelSftp sftp = Utils.getChannelSftp(session);
    try {
      sftp.put(new ByteArrayInputStream(payload.getBytes()), path + "/" + filename);
    } finally {
      sftp.exit();
      session.disconnect();
      LOG.info("sftp exit");
    }
  }

}
