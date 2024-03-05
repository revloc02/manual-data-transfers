package forest.colver.datatransfer.sftp;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

  static final Logger LOG = LoggerFactory.getLogger(Utils.class);
  private static final JSch JSCH = new JSch();

  private Utils() {
    // https://rules.sonarsource.com/java/RSPEC-1118/
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated.");
  }

  static ChannelSftp getChannelSftp(Session session) throws JSchException {
    Channel channel = session.openChannel("sftp");
    channel.connect();
    LOG.info("channel connect");
    return (ChannelSftp) channel;
  }

  static Session getSession(String username, String password, String host)
      throws JSchException {
    Session session = JSCH.getSession(username, host, 22);
    session.setPassword(password);
    LOG.info("sftp password auth");
    JSCH.setKnownHosts(SftpOperations.class.getResourceAsStream("/knownhosts"));
    session.connect();
    LOG.info("session connect");
    return session;
  }
}
