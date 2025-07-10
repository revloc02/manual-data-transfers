package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.aws.S3Operations.s3Delete;
import static forest.colver.datatransfer.aws.S3Operations.s3List;
import static forest.colver.datatransfer.aws.S3Operations.s3ListDeleteMarkers;
import static forest.colver.datatransfer.aws.S3Operations.s3ListResponse;
import static forest.colver.datatransfer.aws.S3Operations.s3ListVersions;
import static forest.colver.datatransfer.aws.S3Operations.s3Put;
import static forest.colver.datatransfer.aws.Utils.S3_INTERNAL;
import static forest.colver.datatransfer.aws.Utils.getEmxNpCreds;
import static forest.colver.datatransfer.aws.Utils.getEmxSbCreds;
import static forest.colver.datatransfer.aws.Utils.getS3Client;
import static forest.colver.datatransfer.config.Utils.getDefaultPayload;
import static forest.colver.datatransfer.config.Utils.getTimeStampFilename;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.awaitility.Awaitility.await;

import forest.colver.datatransfer.aws.S3Operations;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.S3Exception;

class ZzzLearningS3VersioningTests {
  private static final Logger LOG = LoggerFactory.getLogger(ZzzLearningS3VersioningTests.class);

  /**
   * S3 versions research. This interacts with an S3 that has versioning enabled. It places a file,
   * deletes it, and then checks if the versions can be seen. (Remember to get creds first using
   * `aws configure sso`.)
   */
  @Test
  void testS3DeleteButVersionsRemain() { // earliest version observed on console is: 3 Feb 2025
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      var keyPrefix = "revloc02/target/versions-remain";
      var objectKey = keyPrefix + "/remain" + getTimeStampFilename() + ".txt";
      LOG.info("...place a file...");
      s3Put(s3Client, S3_INTERNAL, objectKey, getDefaultPayload());

      LOG.info("...check the file arrived...");
      var objects = s3List(creds, S3_INTERNAL, keyPrefix);
      assertThat(objects).hasSizeGreaterThanOrEqualTo(1); // this actually isn't a good check

      LOG.info("...delete the file...");
      s3Delete(creds, S3_INTERNAL, objectKey);

      LOG.info("...check for versions, can the versions be seen? Yes...");
      s3ListVersions(s3Client, S3_INTERNAL, keyPrefix);
    }
  }

  /**
   * S3 versions research. This test places a file with the same filename on an S3 that has
   * versioning enabled and leaves the versions in place. (Remember to get creds first using `aws
   * configure sso`.)
   */
  @Test
  void testS3MultipleVersionsSameName() { // earliest version observed on console is: 3 Feb 2025
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      var keyPrefix = "revloc02/target/same-name";
      var objectKey = keyPrefix + "/text.txt";
      LOG.info("...place a file...");
      s3Put(s3Client, S3_INTERNAL, objectKey, getDefaultPayload());

      LOG.info("...check the file arrived...");
      var objects = s3List(creds, S3_INTERNAL, keyPrefix);
      assertThat(objects).hasSizeGreaterThanOrEqualTo(1); // this actually isn't a good check
    }
  }

  /**
   * S3 versions research. This test interacts with an S3 that has versioning enabled. It then
   * places a file, deletes it, then deletes the versioned file, and finally deletes the delete
   * marker. (Remember to get creds first using `aws configure sso`.)
   */
  @Test
  void testS3DeleteVersionsRemoved() { // earliest delete-marker observed on console is: 4 Feb 2025
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      var keyPrefix = "revloc02/target/versions-removed";
      var objectKey = keyPrefix + "/removed" + getTimeStampFilename() + ".txt";
      LOG.info("...place a file...");
      s3Put(s3Client, S3_INTERNAL, objectKey, getDefaultPayload());

      LOG.info("...check the file arrived...");
      var objects = s3List(creds, S3_INTERNAL, keyPrefix);
      assertThat(objects).hasSizeGreaterThanOrEqualTo(1);

      LOG.info("...delete the file...");
      s3Delete(creds, S3_INTERNAL, objectKey);

      LOG.info("...check for versions, can the delete marker be seen? No...");
      var versions = s3ListVersions(s3Client, S3_INTERNAL, keyPrefix);

      LOG.info("...cleanup and delete the file versions...");
      for (var version : versions) {
        S3Operations.s3DeleteVersion(s3Client, S3_INTERNAL, version.key(), version.versionId());
      }

      LOG.info("...check for delete markers, can the delete marker be seen? Yes...");
      var deleteMarkers = s3ListDeleteMarkers(s3Client, S3_INTERNAL, keyPrefix);

      LOG.info("...cleanup and delete the file delete markers...");
      for (var deleteMarker : deleteMarkers) {
        S3Operations.s3DeleteVersion(
            s3Client, S3_INTERNAL, deleteMarker.key(), deleteMarker.versionId());
      }
    }
  }

  /**
   * When the SFTP internal bucket in sandbox had versioning turned on, about 2M (400GB) Emcor
   * objects got versions with a versionId of null. This is an attempt at cleaning it up. (Remember
   * to get creds first using `aws configure sso`.)
   */
  @Test
  void listAndDeleteSpecificVersionedObjects() {
    var creds = getEmxSbCreds();
    var bucket = "emx-sandbox-sftp-internal";
    var keyPrefix = "";
    var deleteStartsWith = "emx2-core-test/";
    try (var s3Client = getS3Client(creds)) {
      var objectCount = 0;
      var response = s3ListResponse(s3Client, bucket, keyPrefix, 1000);
      if (response.hasContents()) {
        LOG.info("response.contents().size(): {}", response.contents().size());
        for (var object : response.contents()) {
          LOG.info("object: {}", object.key());
          objectCount++;
        }
      }
      LOG.info("objectCount: {}", objectCount);

      var versionCount = 0;
      var deleteMarkerCount = 0;
      // iterations | time | versionCount and deleteMarkerCount
      // 1 | 1:54 | 666 and 667
      // 16 | 30:22 | 10,672 and 10,672
      // 32 | 1:00:00 | 21,344 and 21,344
      // 64 | 2:13:00 | 42,464 and 42,798
      // 128 | 4:06:00 | 85,152 and 85,486
      // 256 | 8:06:00 | 170,752 and 170,752
      for (var i = 0; i < 16; i++) {
        var versions = s3ListVersions(s3Client, bucket, keyPrefix);
        if (!versions.isEmpty()) {
          LOG.info("versions.size(): {}", versions.size());
          for (var version : versions) {
            LOG.info(
                "version.key(): {} ======= version.versionId(): {}",
                version.key(),
                version.versionId());
            if (version.key().startsWith(deleteStartsWith)) {
              S3Operations.s3DeleteVersion(s3Client, bucket, version.key(), version.versionId());
              versionCount++;
            }
          }
        }

        var deleteMarkers = s3ListDeleteMarkers(s3Client, bucket, keyPrefix);
        if (!deleteMarkers.isEmpty()) {
          LOG.info("deleteMarkers.size(): {}", deleteMarkers.size());
          for (var deleteMarker : deleteMarkers) {
            LOG.info(
                "deleteMarker.key(): {} ======= deleteMarker.versionId(): {}",
                deleteMarker.key(),
                deleteMarker.versionId());
            if (deleteMarker.key().startsWith(deleteStartsWith)) {
              S3Operations.s3DeleteVersion(
                  s3Client, bucket, deleteMarker.key(), deleteMarker.versionId());
              deleteMarkerCount++;
            }
          }
        }
      }
      LOG.info("versions deleted: {}", versionCount);
      LOG.info("deleteMarkers deleted: {}", deleteMarkerCount);
    }
  }

  /** Non-prod internal bucket list of objects and versioned objects. */
  @Test
  void listS3ObjectsNonprodInternal() {
    var creds = getEmxNpCreds();
    var count = 0;
    var bucket = "emx-stage-sftp-internal";
    var keyPrefix = "";
    try (var s3Client = getS3Client(creds)) {
      var response = s3ListResponse(s3Client, bucket, keyPrefix, 1000);
      if (response.hasContents()) {
        LOG.info("response.contents().size(): {}", response.contents().size());
        for (var object : response.contents()) {
          LOG.info("object: {}", object.key());
          count++;
        }
      }
      LOG.info("count: {}", count);

      var versions = s3ListVersions(s3Client, bucket, keyPrefix);
      if (!versions.isEmpty()) {
        LOG.info("versions.size(): {}", versions.size());
        for (var version : versions) {
          LOG.info("version: {}", version);
        }
      }
    }
  }

  /**
   * Retrieves an S3 object from a versioned bucket using a specific version ID. Nothing fancy here,
   * just a sanity check.
   */
  @Test
  void getS3ObjectWithVersionFromVersionedBucket() throws IOException {
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      var keyPrefix = "revloc02/target/versioned";
      var objectKey = keyPrefix + "/versioned-" + getTimeStampFilename() + ".txt";
      LOG.info("...place a file...");
      var versionId = s3Put(s3Client, S3_INTERNAL, objectKey, getDefaultPayload());

      LOG.info("...check the file arrived...");
      var objects = s3List(creds, S3_INTERNAL, keyPrefix);
      assertThat(objects).hasSizeGreaterThanOrEqualTo(1);

      LOG.info("...get the file with versionId: {}", versionId);
      var response = S3Operations.s3Retrieve(s3Client, S3_INTERNAL, objectKey, versionId);
      assertThat(response).isNotNull();
      var respPayload = new String(response.readAllBytes(), StandardCharsets.UTF_8);
      assertThat(respPayload).isEqualTo(getDefaultPayload());

      LOG.info("...cleanup and delete the file...");
      S3Operations.s3DeleteVersion(s3Client, S3_INTERNAL, objectKey, versionId.orElseThrow());

      LOG.info("...since versionId was used to delete, there should be no deleteMarkers...");
      await()
          .pollInterval(Duration.ofSeconds(3))
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(
              () -> assertThat(s3ListDeleteMarkers(s3Client, S3_INTERNAL, keyPrefix)).isEmpty());
    }
  }

  /**
   * Retrieves an S3 object from a versioned bucket without specifying a version ID. This is
   * expected to retrieve the latest version of the object.
   */
  @Test
  void getS3ObjectWithoutUsingVersionFromVersionedBucket() throws IOException {
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      var keyPrefix = "revloc02/target/version-not-specified";
      var objectKey = keyPrefix + "/version-not-specified-" + getTimeStampFilename() + ".txt";
      LOG.info("...place a file...");
      s3Put(s3Client, S3_INTERNAL, objectKey, getDefaultPayload());

      LOG.info("...check the file arrived...");
      var objects = s3List(creds, S3_INTERNAL, keyPrefix);
      assertThat(objects).hasSizeGreaterThanOrEqualTo(1);

      LOG.info("...put a different version of the file with the same name...");
      s3Put(s3Client, S3_INTERNAL, objectKey, "Different content");

      LOG.info("...retrieve the file, do not include versionId...");
      var response = S3Operations.s3Retrieve(s3Client, S3_INTERNAL, objectKey, Optional.empty());
      assertThat(response).isNotNull();
      var respPayload = new String(response.readAllBytes(), StandardCharsets.UTF_8);
      assertThat(respPayload).isEqualTo("Different content");

      LOG.info("...check for 2 versions...");
      var versions = s3ListVersions(s3Client, S3_INTERNAL, keyPrefix);
      assertThat(versions).hasSizeGreaterThanOrEqualTo(2);

      LOG.info("...cleanup and delete all version IDs...");
      for (var version : versions) {
        S3Operations.s3DeleteVersion(s3Client, S3_INTERNAL, objectKey, version.versionId());
      }
      LOG.info("...assert cleanup...");
      await()
          .pollInterval(Duration.ofSeconds(3))
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(
              () -> assertThat(s3ListVersions(s3Client, S3_INTERNAL, keyPrefix)).isEmpty());
      LOG.info(
          "...if versionId were used to delete, then there should be no deleteMarkers, assert this...");
      await()
          .pollInterval(Duration.ofSeconds(3))
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(
              () -> assertThat(s3ListDeleteMarkers(s3Client, S3_INTERNAL, keyPrefix)).isEmpty());
    }
  }

  /**
   * Attempts to retrieve an S3 object from a non-versioned bucket using a version ID. This should
   * fail, as the bucket is not versioned.
   */
  @Test
  void getS3ObjectWithVersionFromNonVersionedBucket() throws IOException {
    var creds = getEmxSbCreds();
    var bucket = "emx-sandbox-sftp-source-cache"; // non-versioned bucket
    try (var s3Client = getS3Client(creds)) {
      var keyPrefix = "revloc02/target/non-versioned";
      var objectKey = keyPrefix + "/non-versioned-" + getTimeStampFilename() + ".txt";
      LOG.info("...place a file...");
      s3Put(s3Client, bucket, objectKey, getDefaultPayload());

      LOG.info("...check the file arrived...");
      var objects = s3List(creds, bucket, keyPrefix);
      assertThat(objects).hasSizeGreaterThanOrEqualTo(1);

      LOG.info("...first, get the file without versionId...");
      var response = S3Operations.s3Retrieve(s3Client, bucket, objectKey, Optional.empty());
      assertThat(response).isNotNull();
      var respPayload = new String(response.readAllBytes(), StandardCharsets.UTF_8);
      assertThat(respPayload).isEqualTo(getDefaultPayload());

      LOG.info("...now, get the file using a versionId...");
      assertThatExceptionOfType(S3Exception.class)
          .isThrownBy(
              () ->
                  S3Operations.s3Retrieve(
                      s3Client, bucket, objectKey, Optional.of("bogus-version-id")))
          .withMessageContaining("Invalid version id specified");

      LOG.info("...cleanup and delete the file...");
      S3Operations.s3Delete(s3Client, S3_INTERNAL, objectKey);
    }
  }
}
