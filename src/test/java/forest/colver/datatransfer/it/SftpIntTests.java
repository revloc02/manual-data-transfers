package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.aws.AwsUtils.S3_INTERNAL_VERSIONED;
import static forest.colver.datatransfer.aws.AwsUtils.getEmxSbCreds;
import static forest.colver.datatransfer.aws.AwsUtils.getS3Client;
import static forest.colver.datatransfer.aws.S3Operations.s3DeleteVersion;
import static forest.colver.datatransfer.aws.S3Operations.s3List;
import static forest.colver.datatransfer.aws.S3Operations.s3ListDeleteMarkers;
import static forest.colver.datatransfer.aws.S3Operations.s3ListVersions;
import static forest.colver.datatransfer.sftp.SftpOperations.consumeSftpFile;
import static forest.colver.datatransfer.sftp.SftpOperations.consumeSftpFileUseKeyManageSession;
import static forest.colver.datatransfer.sftp.SftpOperations.consumeSftpFileUsePasswordManageSession;
import static forest.colver.datatransfer.sftp.SftpOperations.putSftpFile;
import static forest.colver.datatransfer.sftp.SftpOperations.putSftpFileUseKeyManageSession;
import static forest.colver.datatransfer.sftp.SftpOperations.putSftpFileUsePasswordManageSession;
import static forest.colver.datatransfer.sftp.SftpUtils.SFTP_HOST;
import static forest.colver.datatransfer.sftp.SftpUtils.SFTP_PASSWORD;
import static forest.colver.datatransfer.sftp.SftpUtils.connectChannelSftp;
import static forest.colver.datatransfer.sftp.SftpUtils.getKeySession;
import static forest.colver.datatransfer.sftp.SftpUtils.getPwSession;
import static forest.colver.datatransfer.sftp.SftpUtils.sftpDisconnect;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.awaitility.Awaitility.await;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SftpIntTests {

  static final Logger LOG = LoggerFactory.getLogger(SftpIntTests.class);
  public static final String USERNAME = "revloc02";
  public static final String SFTP_PATH = "target/test";
  public static final String FILENAME = "sftp-test.txt";
  public static final String PAYLOAD = "payload";
  public static final String SFTP_KEY_LOC = "src/main/resources/prvKey";

  @Test
  void testSftpPutConsume_Pw_ManageChannel() throws SftpException, IOException, JSchException {
    var session = getPwSession(SFTP_HOST, USERNAME, SFTP_PASSWORD);
    var sftp = connectChannelSftp(session);
    putSftpFile(sftp, SFTP_PATH, FILENAME, PAYLOAD);
    var contents = consumeSftpFile(sftp, SFTP_PATH, FILENAME);
    assertThat(contents).isEqualTo(PAYLOAD);
  }

  @Test
  void testSftpPutConsume_Key_ManageChannel() throws SftpException, IOException, JSchException {
    var session = getKeySession(SFTP_HOST, USERNAME, SFTP_KEY_LOC);
    var sftp = connectChannelSftp(session);
    putSftpFile(sftp, SFTP_PATH, FILENAME, PAYLOAD);
    var contents = consumeSftpFile(sftp, SFTP_PATH, FILENAME);
    assertThat(contents).isEqualTo(PAYLOAD);
    sftpDisconnect(sftp, session);
  }

  /** Runs a series of AWS SFTP authentication tests, both successes and failures. */
  @Test
  void testAwsSftpAuthFails() throws SftpException, IOException, JSchException {
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
                putSftpFile(sftp, SFTP_PATH, "this-should-not-work.txt", PAYLOAD);
              });

      LOG.info("...do an unsuccessful key auth...");
      assertThatExceptionOfType(JSchException.class)
          .isThrownBy(
              () -> {
                var session =
                    getKeySession(
                        SFTP_HOST, "emx-app-test-keyauth", "src/main/resources/badPrvKey");
                var sftp = connectChannelSftp(session);
                putSftpFile(sftp, SFTP_PATH, "this-should-not-work.txt", PAYLOAD);
              });

      LOG.info("...do a successful password auth...");
      putSftpFileUsePasswordManageSession(
          SFTP_HOST, "revloc02a", SFTP_PASSWORD, SFTP_PATH, FILENAME, PAYLOAD);
      var contents =
          consumeSftpFileUsePasswordManageSession(
              SFTP_HOST, "revloc02a", SFTP_PASSWORD, SFTP_PATH, FILENAME);
      assertThat(contents).isEqualTo(PAYLOAD);

      LOG.info("...do a successful key auth...");
      putSftpFileUseKeyManageSession(
          SFTP_HOST, "revloc02", SFTP_KEY_LOC, SFTP_PATH, FILENAME, PAYLOAD);
      contents =
          consumeSftpFileUseKeyManageSession(
              SFTP_HOST, "revloc02", SFTP_KEY_LOC, SFTP_PATH, FILENAME);
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
        SFTP_HOST, USERNAME, SFTP_PASSWORD, SFTP_PATH, FILENAME, PAYLOAD);
    var contents =
        consumeSftpFileUsePasswordManageSession(
            SFTP_HOST, USERNAME, SFTP_PASSWORD, SFTP_PATH, FILENAME);
    assertThat(contents).isEqualTo(PAYLOAD);
  }

  /** This tests the SFTP operations that create a session using key auth, for the one operation. */
  @Test
  void testSftpPutConsume_UseKeyMakeSession() throws JSchException, SftpException, IOException {
    putSftpFileUseKeyManageSession(SFTP_HOST, USERNAME, SFTP_KEY_LOC, SFTP_PATH, FILENAME, PAYLOAD);
    var contents =
        consumeSftpFileUseKeyManageSession(SFTP_HOST, USERNAME, SFTP_KEY_LOC, SFTP_PATH, FILENAME);
    assertThat(contents).isEqualTo(PAYLOAD);
  }

  /**
   * Tests AWS SFTP version behavior by uploading the same file multiple times rapidly via SFTP to
   * understand how AWS Transfer Server handles versioning on S3_INTERNAL_VERSIONED bucket.
   *
   * <p>Conclusion: AWS Transfer Server properly handles rapid file uploads with the same name by
   * creating distinct versioned objects on S3, even when uploads occur within the same second
   * through a single SFTP connection. Each upload creates a new version with a unique versionId and
   * ETag, demonstrating that S3 versioning works correctly through the AWS Transfer SFTP interface.
   */
  @Test
  void testAwsSftpVersionBehavior() throws JSchException, SftpException, IOException {
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      var username = "revloc02-v";
      var filename = "version-test.txt";
      var keyPrefix = username + "/" + SFTP_PATH + "/";
      var objectKey = keyPrefix + filename;

      LOG.info("...cleanup any leftover versions from previous failed tests...");
      var existingVersions = s3ListVersions(s3Client, S3_INTERNAL_VERSIONED, keyPrefix);
      var existingFiles = existingVersions.stream().filter(v -> v.size() > 0).toList();
      for (var version : existingFiles) {
        s3DeleteVersion(s3Client, S3_INTERNAL_VERSIONED, objectKey, version.versionId());
      }
      var existingMarkers = s3ListDeleteMarkers(s3Client, S3_INTERNAL_VERSIONED, keyPrefix);
      for (var marker : existingMarkers) {
        if (marker.key().equals(objectKey)) {
          s3DeleteVersion(s3Client, S3_INTERNAL_VERSIONED, objectKey, marker.versionId());
        }
      }

      LOG.info("...upload the same file 5 times rapidly via SFTP with different content...");
      var session = getPwSession(SFTP_HOST, username, SFTP_PASSWORD);
      var sftp = connectChannelSftp(session);
      putSftpFile(sftp, SFTP_PATH, filename, "Version 1 content");
      putSftpFile(sftp, SFTP_PATH, filename, "Version 2 content");
      putSftpFile(sftp, SFTP_PATH, filename, "Version 3 content");
      putSftpFile(sftp, SFTP_PATH, filename, "Version 4 content");
      putSftpFile(sftp, SFTP_PATH, filename, "Version 5 content");
      sftpDisconnect(sftp, session);

      LOG.info("...check that there is one file in a list...");
      var objects = s3List(creds, S3_INTERNAL_VERSIONED, keyPrefix);
      var filesOnly = objects.stream().filter(obj -> obj.size() > 0).toList();
      assertThat(filesOnly).asList().hasSize(1);

      LOG.info("...check how many versions were created...");
      var allVersions = s3ListVersions(s3Client, S3_INTERNAL_VERSIONED, keyPrefix);
      var fileVersions = allVersions.stream().filter(v -> v.size() > 0).toList();
      LOG.info(
          "AWS SFTP created {} version(s) for {} rapid uploads of the same file",
          fileVersions.size(),
          5);
      assertThat(fileVersions).asList().hasSize(5);

      LOG.info("...cleanup and delete all version IDs...");
      for (var version : fileVersions) {
        s3DeleteVersion(s3Client, S3_INTERNAL_VERSIONED, objectKey, version.versionId());
      }
      LOG.info("...assert cleanup...");
      await()
          .pollInterval(Duration.ofSeconds(3))
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(
              () -> {
                var remaining =
                    s3ListVersions(s3Client, S3_INTERNAL_VERSIONED, keyPrefix).stream()
                        .filter(v -> v.size() > 0)
                        .toList();
                assertThat(remaining).asList().isEmpty();
              });
      LOG.info(
          "...if versionId were used to delete, then there should be no deleteMarkers, assert this...");
      await()
          .pollInterval(Duration.ofSeconds(3))
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(
              () -> {
                var deleteMarkers =
                    s3ListDeleteMarkers(s3Client, S3_INTERNAL_VERSIONED, keyPrefix).stream()
                        .filter(m -> m.key().equals(objectKey))
                        .toList();
                assertThat(deleteMarkers).asList().isEmpty();
              });
    }
  }
}
