package forest.colver.datatransfer.sftp;

import static forest.colver.datatransfer.config.Utils.userCreds;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import java.io.ByteArrayInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

  static final Logger LOG = LoggerFactory.getLogger(Utils.class);
  private static final JSch JSCH = new JSch();
  private static final String KNOWNHOSTS = userCreds.getProperty("sftp.knownhosts");
  public static final String SFTP_HOST = userCreds.getProperty("sftp.host");
  public static final String SFTP_PASSWORD = userCreds.getProperty("sftp.password");

  private Utils() {
    // https://rules.sonarsource.com/java/RSPEC-1118/
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated.");
  }

  public static ChannelSftp connectChannelSftp(Session session) throws JSchException {
    Channel channel = session.openChannel("sftp");
    channel.connect();
    LOG.info("sftp channel connect");
    return (ChannelSftp) channel;
  }

  public static void sftpDisconnect(ChannelSftp sftp, Session session) {
    sftp.exit();
    LOG.info("sftp channel exit");
    session.disconnect();
    LOG.info("session disconnect");
  }

  public static Session getSession(String host, String username, String password)
      throws JSchException {
    Session session = JSCH.getSession(username, host, 22);
    session.setPassword(password);
    LOG.info("sftp password auth");
    JSCH.setKnownHosts(new ByteArrayInputStream(KNOWNHOSTS.getBytes()));
    session.connect();
    LOG.info("session connect");
    return session;
  }
}
