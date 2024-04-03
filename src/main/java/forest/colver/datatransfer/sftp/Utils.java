package forest.colver.datatransfer.sftp;

import static forest.colver.datatransfer.config.Utils.userCreds;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.Identity;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.KeyPair;
import com.jcraft.jsch.Session;
import java.io.ByteArrayInputStream;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

  public static final String SFTP_EXIT = "sftp exit";
  static final Logger LOG = LoggerFactory.getLogger(Utils.class);
  private static final JSch JSCH = new JSch();
  private static final String KNOWNHOSTS = userCreds.getProperty("sftp.knownhosts");
  public static final String SFTP_HOST = userCreds.getProperty("sftp.host");
  public static final String SFTP_PASSWORD = userCreds.getProperty("sftp.password");
  public static final String SFTP_KEY = userCreds.getProperty("sftp.key");

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
    session.disconnect();
    LOG.info(SFTP_EXIT);
  }

  public static Session getPwSession(String host, String username, String password)
      throws JSchException {
    Session session = JSCH.getSession(username, host, 22);
    session.setPassword(password);
    LOG.info("sftp password auth");
    JSCH.setKnownHosts(new ByteArrayInputStream(KNOWNHOSTS.getBytes()));
    session.connect();
    LOG.info("session connect");
    return session;
  }

  public static Session getKeySession(String host, String username, String prvKeyLoc)
      throws JSchException {
    Session session = JSCH.getSession(username, host, 22);
//    JSCH.addIdentity(new InMemoryIdentity(prvKey), null);
    JSCH.addIdentity(prvKeyLoc);
    LOG.info("sftp ssh key auth");
    JSCH.setKnownHosts(new ByteArrayInputStream(KNOWNHOSTS.getBytes()));
    session.connect();
    LOG.info("session connect");
    return session;
  }

  /**
   * This class allows an ssh key, an identity, to be created in memory to be used for
   * authentication. (Currently not being used, but keeping it for reference.)
   */
  public static class InMemoryIdentity implements Identity {

    private static final String DEFAULT_PUBLIC_KEY = "publicKeyNotNeededUsingDefault";
    private static final String DEFAULT_KEY_NAME = "customKeyNamesNotCurrentlyImplemented";
    private final KeyPair keyPair;

    /**
     * Example usage: JSCH.addIdentity(new InMemoryIdentity(prvKey), null);
     * @param prvkey a string representation of the private key
     */
    public InMemoryIdentity(String prvkey) throws JSchException {
      Objects.requireNonNull(prvkey);
      if (prvkey.isEmpty()) {
        throw new IllegalArgumentException("prvkey cannot be empty");
      }
      var jsch = new JSch();
      this.keyPair = KeyPair.load(jsch, prvkey.getBytes(), DEFAULT_PUBLIC_KEY.getBytes());
    }

    @Override
    public boolean setPassphrase(byte[] passphrase) {
      return keyPair.decrypt(passphrase);
    }

    @Override
    public byte[] getPublicKeyBlob() {
      return keyPair.getPublicKeyBlob();
    }

    @Override
    public byte[] getSignature(byte[] data) {
      return keyPair.getSignature(data);
    }

    @Override
    public String getAlgName() {
      if (keyPair.getKeyType() == 1) {
        return "ssh-dss";
      }
      return "ssh-rsa";
    }

    @Override
    public String getName() {
      return DEFAULT_KEY_NAME;
    }

    @Override
    public boolean isEncrypted() {
      return keyPair.isEncrypted();
    }

    @Override
    public void clear() {
      keyPair.dispose();
    }
  }
}
