package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.aws.S3Operations.s3Consume;
import static forest.colver.datatransfer.aws.S3Operations.s3Copy;
import static forest.colver.datatransfer.aws.S3Operations.s3CopyAll;
import static forest.colver.datatransfer.aws.S3Operations.s3CountAll;
import static forest.colver.datatransfer.aws.S3Operations.s3Delete;
import static forest.colver.datatransfer.aws.S3Operations.s3DeleteAll;
import static forest.colver.datatransfer.aws.S3Operations.s3Get;
import static forest.colver.datatransfer.aws.S3Operations.s3Head;
import static forest.colver.datatransfer.aws.S3Operations.s3List;
import static forest.colver.datatransfer.aws.S3Operations.s3ListContResponse;
import static forest.colver.datatransfer.aws.S3Operations.s3ListDeleteMarkers;
import static forest.colver.datatransfer.aws.S3Operations.s3ListResponse;
import static forest.colver.datatransfer.aws.S3Operations.s3ListVersions;
import static forest.colver.datatransfer.aws.S3Operations.s3Move;
import static forest.colver.datatransfer.aws.S3Operations.s3MoveAll;
import static forest.colver.datatransfer.aws.S3Operations.s3Put;
import static forest.colver.datatransfer.aws.Utils.S3_INTERNAL;
import static forest.colver.datatransfer.aws.Utils.S3_TARGET_CUSTOMER;
import static forest.colver.datatransfer.aws.Utils.getEmxSbCreds;
import static forest.colver.datatransfer.aws.Utils.getS3Client;
import static forest.colver.datatransfer.config.Utils.getDefaultPayload;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import forest.colver.datatransfer.aws.S3Operations;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/** Integration Tests for AWS S3 */
class AwsS3IntTests {

  private static final Logger LOG = LoggerFactory.getLogger(AwsS3IntTests.class);

  // Pass parameters to the S3 operation

  /** Each S3 operation uses the creds to create its own S3Client. */
  @Test
  void testS3Put_PassCredsAndParams() {
    // put a file
    var objectKey = "revloc02/source/test/test.txt";
    var creds = getEmxSbCreds();
    s3Put(creds, S3_INTERNAL, objectKey, getDefaultPayload());

    // verify the file is there
    var objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects.size()).isOne();
    assertThat(objects.get(0).key()).isEqualTo(objectKey);
    // just testing the Put, so purposely not checking the payload value

    // delete the file
    s3Delete(creds, S3_INTERNAL, objectKey);

    // verify the file is gone
    objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects).isEmpty();
  }

  @Test
  void testS3Put_PassS3ClientAndParams() {
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      // put a file
      var objectKey = "revloc02/source/test/test.txt";
      s3Put(s3Client, S3_INTERNAL, objectKey, getDefaultPayload());

      // verify the file is there
      var objects = s3List(s3Client, S3_INTERNAL, objectKey);
      assertThat(objects.size()).isOne();
      assertThat(objects.get(0).key()).isEqualTo(objectKey);
      // just testing the Put, so purposely not checking the payload value

      // delete the file
      s3Delete(s3Client, S3_INTERNAL, objectKey);

      // verify the file is gone
      objects = s3List(s3Client, S3_INTERNAL, objectKey);
      assertThat(objects).isEmpty();
    }
  }

  @Test
  void testS3Put_PassCredsAndParamsWithMetadata() {
    // put a file
    var objectKey = "revloc02/source/test/test.txt";
    var creds = getEmxSbCreds();
    var metadata = Map.of("key", "value", "key2", "value2");
    s3Put(creds, S3_INTERNAL, objectKey, getDefaultPayload(), metadata);

    // verify the file is there
    var objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects.size()).isOne();
    assertThat(objects.get(0).key()).isEqualTo(objectKey);
    // just testing the Put, so purposely not checking the payload value

    // use head to verify metadata
    var headObjectResponse = s3Head(creds, S3_INTERNAL, objectKey);
    assertThat(headObjectResponse.metadata()).containsEntry("key", "value");
    assertThat(headObjectResponse.metadata()).containsEntry("key2", "value2");

    // delete the file
    s3Delete(creds, S3_INTERNAL, objectKey);

    // verify the file is gone
    objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects).isEmpty();
  }

  @Test
  void testS3Put_PassS3ClientAndParamsWithMetadata() {
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      // put a file
      var objectKey = "revloc02/source/test/test.txt";
      var metadata = Map.of("key", "value", "key2", "value2");
      s3Put(s3Client, S3_INTERNAL, objectKey, getDefaultPayload(), metadata);

      // verify the file is there
      var objects = s3List(s3Client, S3_INTERNAL, objectKey);
      assertThat(objects.size()).isOne();
      assertThat(objects.get(0).key()).isEqualTo(objectKey);
      // just testing the Put, so purposely not checking the payload value

      // use head to verify metadata
      var headObjectResponse = s3Head(s3Client, S3_INTERNAL, objectKey);
      assertThat(headObjectResponse.metadata()).containsEntry("key", "value");
      assertThat(headObjectResponse.metadata()).containsEntry("key2", "value2");

      // delete the file
      s3Delete(s3Client, S3_INTERNAL, objectKey);

      // verify the file is gone
      objects = s3List(s3Client, S3_INTERNAL, objectKey);
      assertThat(objects).isEmpty();
    }
  }

  @Test
  void testS3Consume() {
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      LOG.info("...put a file...");
      var objectKey = "revloc02/source/test/test.txt";
      s3Put(s3Client, S3_INTERNAL, objectKey, getDefaultPayload());

      LOG.info("...verify the file is there...");
      var objects = s3List(s3Client, S3_INTERNAL, objectKey);
      assertThat(objects.size()).isOne();
      assertThat(objects.get(0).key()).isEqualTo(objectKey);

      LOG.info("...consume the file...");
      s3Consume(s3Client, S3_INTERNAL, objectKey);

      LOG.info("...verify the file is gone...");
      objects = s3List(s3Client, S3_INTERNAL, objectKey);
      assertThat(objects).isEmpty();
    }
  }

  @Test
  void testS3Copy_PassClient() throws IOException {
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      LOG.info("...place a file...");
      var objectKey = "revloc02/source/test/test.txt";
      var payload = getDefaultPayload();
      s3Put(s3Client, S3_INTERNAL, objectKey, payload);

      LOG.info("...verify the object is on the source...");
      var objects = s3List(s3Client, S3_INTERNAL, objectKey);
      assertThat(objects.size()).isOne();
      assertThat(objects.get(0).key()).isEqualTo(objectKey);

      LOG.info("...copy object...");
      var destKey = "blake/inbound/dev/some-bank/ack/testCopied.txt";
      s3Copy(s3Client, S3_INTERNAL, objectKey, S3_TARGET_CUSTOMER, destKey);

      LOG.info("...verify the copy is in the new location...");
      objects = s3List(s3Client, S3_TARGET_CUSTOMER, destKey);
      assertThat(objects.size()).isOne();
      assertThat(objects.get(0).key()).isEqualTo(destKey);

      LOG.info("...check the contents...");
      var response = s3Get(s3Client, S3_TARGET_CUSTOMER, destKey);
      LOG.info("response={}", response.response());
      LOG.info("statusCode={}", response.response().sdkHttpResponse().statusCode());
      var respPayload = new String(response.readAllBytes(), StandardCharsets.UTF_8);
      assertThat(respPayload).isEqualTo(payload);

      LOG.info("...verify the object is still on the source...");
      objects = s3List(s3Client, S3_INTERNAL, objectKey);
      assertThat(objects.size()).isOne();
      assertThat(objects.get(0).key()).isEqualTo(objectKey);

      LOG.info("...cleanup and delete the files...");
      response.close();
      s3Delete(s3Client, S3_INTERNAL, objectKey);
      s3Delete(s3Client, S3_TARGET_CUSTOMER, destKey);
    }
  }

  @Test
  void testS3Copy_PassCreds() throws IOException {
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      // place a file
      var objectKey = "revloc02/source/test/test.txt";
      var payload = getDefaultPayload();
      s3Put(s3Client, S3_INTERNAL, objectKey, payload);

      // verify the file is in the source
      var objects = s3List(s3Client, S3_INTERNAL, objectKey);
      assertThat(objects.size()).isOne();
      assertThat(objects.get(0).key()).isEqualTo(objectKey);

      // copy file
      var destKey = "blake/inbound/dev/some-bank/ack/testCopied.txt";
      s3Copy(creds, S3_INTERNAL, objectKey, S3_TARGET_CUSTOMER, destKey);

      // verify the copy is in the new location
      objects = s3List(s3Client, S3_TARGET_CUSTOMER, destKey);
      assertThat(objects.size()).isOne();
      assertThat(objects.get(0).key()).isEqualTo(destKey);

      // check the contents
      var response = s3Get(s3Client, S3_TARGET_CUSTOMER, destKey);
      LOG.info("response={}", response.response());
      LOG.info("statusCode={}", response.response().sdkHttpResponse().statusCode());
      var respPayload = new String(response.readAllBytes(), StandardCharsets.UTF_8);
      assertThat(respPayload).isEqualTo(payload);

      // cleanup and delete the files
      response.close();
      s3Delete(s3Client, S3_INTERNAL, objectKey);
      s3Delete(s3Client, S3_TARGET_CUSTOMER, destKey);
    }
  }

  @Test
  void testS3Delete() {
    // put a file
    var objectKey = "revloc02/source/test/test.txt";
    var creds = getEmxSbCreds();
    s3Put(creds, S3_INTERNAL, objectKey, getDefaultPayload());

    // verify the file is there
    var objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects.size()).isOne();
    assertThat(objects.get(0).key()).isEqualTo(objectKey);

    // delete the file
    s3Delete(creds, S3_INTERNAL, objectKey);

    // verify the file is gone
    objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects).isEmpty();
  }

  /** Tests s3List that requires creds and returns a List of objects. */
  @Test
  void testS3List() {
    var creds = getEmxSbCreds();
    var objectKey = "revloc02/target/test/mdtTest1.txt";
    s3Put(creds, S3_INTERNAL, objectKey, getDefaultPayload());
    var objects = s3List(creds, S3_INTERNAL, "revloc02/target/test");
    assertThat(objects.get(0).key()).isEqualTo(objectKey);
    assertThat(objects.get(0).size()).isEqualTo(40L);
    s3Delete(creds, S3_INTERNAL, objectKey);
  }

  /** Tests s3List that requires S3 Client and maxKeys, and returns a List of objects. */
  @Test
  void testS3ListWithMaxkeys() {
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      var objectKey = "revloc02/target/test/mdtTest1.txt";
      s3Put(s3Client, S3_INTERNAL, objectKey, getDefaultPayload());
      var objects = s3List(s3Client, S3_INTERNAL, "revloc02/target/test", 10);
      assertThat(objects.get(0).key()).isEqualTo(objectKey);
      assertThat(objects.get(0).size()).isEqualTo(40L);
      s3Delete(s3Client, S3_INTERNAL, objectKey);
    }
  }

  /** Tests s3List that requires creds and returns a ListObjectResponse. */
  @Test
  void testS3ListResponse() {
    var creds = getEmxSbCreds();
    var objectKey = "revloc02/target/test/mdtTest1.txt";
    s3Put(creds, S3_INTERNAL, objectKey, getDefaultPayload());
    var response = s3ListResponse(creds, S3_INTERNAL, "revloc02/target/test");
    assertThat(response.contents().get(0).key()).isEqualTo(objectKey);
    assertThat(response.contents().get(0).size()).isEqualTo(40L);
    s3Delete(creds, S3_INTERNAL, objectKey);
  }

  /** Tests s3List that requires S3 Client and maxKeys, and returns a ListObjectsResponse. */
  @Test
  void testS3ListResponseWithMaxKeys() {
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      var objectKey = "revloc02/target/test/mdtTest1.txt";
      s3Put(s3Client, S3_INTERNAL, objectKey, getDefaultPayload());
      var response = s3ListResponse(s3Client, S3_INTERNAL, "revloc02/target/test", 50);
      assertThat(response.contents().get(0).key()).isEqualTo(objectKey);
      assertThat(response.contents().get(0).size()).isEqualTo(40L);
      s3Delete(s3Client, S3_INTERNAL, objectKey);
    }
  }

  /**
   * This is not a unit test, rather it is an awssdk behavior discovery. When using the awssdk, and
   * sending a parameter of maxKeysReq more than 1000, you still only get 1000. Leaving it here for
   * posterity.
   */
  @Test
  void testS3ListWithParamGreaterThan1000() {
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      LOG.info("...place several files...");
      var numFiles = 1100;
      var keyPrefix = "revloc02/source/test/";
      for (var i = 0; i < numFiles; i++) {
        var objectKey = keyPrefix + "test-" + i + ".txt";
        var payload = getDefaultPayload() + " " + i;
        s3Put(s3Client, S3_INTERNAL, objectKey, payload);
      }

      LOG.info("...try to list more than 1000 files, but see that there only is 1000...");
      var objects = s3List(s3Client, S3_INTERNAL, keyPrefix, 1010);
      assertThat(objects).hasSize(1000);

      LOG.info("...cleanup...");
      s3DeleteAll(s3Client, S3_INTERNAL, keyPrefix);
    }
  }

  /** Testing S3 List with more than 1000 items in the list. */
  @Test
  void testS3ListWithMoreThan1000Items() {
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      LOG.info("...place several files...");
      var numFiles = 1100;
      var keyPrefix = "revloc02/source/test/";
      for (var i = 0; i < numFiles; i++) {
        var objectKey = keyPrefix + "test-" + i + ".txt";
        var payload = getDefaultPayload() + " " + i;
        s3Put(s3Client, S3_INTERNAL, objectKey, payload);
      }

      LOG.info("...list 1000 objects, then list the rest of the objects...");
      var response = s3ListResponse(s3Client, S3_INTERNAL, keyPrefix, 1000);
      response =
          s3ListContResponse(s3Client, S3_INTERNAL, keyPrefix, response.nextContinuationToken());
      LOG.info("...check the the last list is 100 objects...");
      assertThat(response.contents()).hasSizeGreaterThanOrEqualTo(100);

      LOG.info("...cleanup...");
      s3DeleteAll(s3Client, S3_INTERNAL, keyPrefix);
    }
  }

  // Pass a PutObjectRequest to the S3 operation

  /** Each S3 operation uses the creds to create its own S3Client. */
  @Test
  void testS3Put_PassCredsPassPutObjReq() {
    // put a file
    var objectKey = "revloc02/source/test/test.txt";
    var creds = getEmxSbCreds();
    var putObjectRequest = PutObjectRequest.builder().bucket(S3_INTERNAL).key(objectKey).build();
    s3Put(creds, getDefaultPayload(), putObjectRequest);

    // verify the file is there
    var objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects.size()).isOne();
    assertThat(objects.get(0).key()).isEqualTo(objectKey);
    // just testing the Put, so purposely not checking the payload value

    // delete the file
    s3Delete(creds, S3_INTERNAL, objectKey);

    // verify the file is gone
    objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects).isEmpty();
  }

  /** One S3Client is created and then passed to each of the S3 operations. */
  @Test
  void testS3Put_PassClientPassPutObjReq() {
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      // put a file
      var objectKey = "revloc02/source/test/test.txt";
      var putObjectRequest = PutObjectRequest.builder().bucket(S3_INTERNAL).key(objectKey).build();
      s3Put(s3Client, getDefaultPayload(), putObjectRequest);

      // verify the file is there
      var objects = s3List(s3Client, S3_INTERNAL, objectKey);
      assertThat(objects.size()).isOne();
      assertThat(objects.get(0).key()).isEqualTo(objectKey);
      // just testing the Put, so purposely not checking the payload value

      // delete the file
      s3Delete(s3Client, S3_INTERNAL, objectKey);

      // verify the file is gone
      objects = s3List(s3Client, S3_INTERNAL, objectKey);
      assertThat(objects).isEmpty();
    }
  }

  @Test
  void testS3Put_PassClientPassObjReqWithTagging() throws IOException {
    try (var s3Client = getS3Client(getEmxSbCreds())) {
      // put a file
      var objectKey = "revloc02/source/test/test.txt";
      var tagging = "expirableObject=true";
      var putObjectRequest =
          PutObjectRequest.builder().bucket(S3_INTERNAL).key(objectKey).tagging(tagging).build();
      s3Put(s3Client, getDefaultPayload(), putObjectRequest);

      // verify the file is there
      var objects = s3List(s3Client, S3_INTERNAL, objectKey);
      assertThat(objects.size()).isOne();
      assertThat(objects.get(0).key()).isEqualTo(objectKey);

      // retrieve object and check it
      var response = s3Get(s3Client, S3_INTERNAL, objectKey);
      String payload = new String(response.readAllBytes());
      assertThat(payload).isEqualTo(getDefaultPayload());
      assertThat(response.response().tagCount()).isOne();
      response.close();

      // check object tag values
      var getTags = GetObjectTaggingRequest.builder().bucket(S3_INTERNAL).key(objectKey).build();
      var result = s3Client.getObjectTagging(getTags);
      assertThat(result.tagSet().get(0).key()).isEqualTo("expirableObject");
      assertThat(result.tagSet().get(0).value()).isEqualTo("true");

      // delete the file
      s3Delete(s3Client, S3_INTERNAL, objectKey);

      // verify the file is gone
      objects = s3List(s3Client, S3_INTERNAL, objectKey);
      assertThat(objects).isEmpty();
    }
  }

  @Test
  void testS3Get() throws IOException {
    try (var s3Client = getS3Client(getEmxSbCreds())) {
      // put a file
      var objectKey = "revloc02/source/test/test.txt";
      var putObjectRequest = PutObjectRequest.builder().bucket(S3_INTERNAL).key(objectKey).build();
      s3Put(s3Client, getDefaultPayload(), putObjectRequest);

      // verify the file is there
      var objects = s3List(s3Client, S3_INTERNAL, objectKey);
      assertThat(objects.size()).isOne();
      assertThat(objects.get(0).key()).isEqualTo(objectKey);

      // use the s3Get method and check that it works
      try (var response = s3Get(s3Client, S3_INTERNAL, objectKey)) {
        String payload = new String(response.readAllBytes());
        assertThat(payload).isEqualTo(getDefaultPayload());
      }

      // delete the file
      s3Delete(s3Client, S3_INTERNAL, objectKey);

      // verify the file is gone
      objects = s3List(s3Client, S3_INTERNAL, objectKey);
      assertThat(objects).isEmpty();
    }
  }

  @Test
  void testS3Head_PassCredsPassPutObjReqWithMetadata() {
    // put a file
    var objectKey = "revloc02/source/test/test.txt";
    var creds = getEmxSbCreds();
    var metadata = Map.of("key", "value");
    var putObjectRequest =
        PutObjectRequest.builder().bucket(S3_INTERNAL).key(objectKey).metadata(metadata).build();
    s3Put(creds, getDefaultPayload(), putObjectRequest);

    // verify the file is there
    var objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects.size()).isOne();
    assertThat(objects.get(0).key()).isEqualTo(objectKey);

    // use head to verify metadata
    var headObjectResponse = s3Head(creds, S3_INTERNAL, objectKey);
    assertThat(headObjectResponse.metadata()).containsEntry("key", "value");

    // delete the file
    s3Delete(creds, S3_INTERNAL, objectKey);

    // verify the file is gone
    objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects).isEmpty();
  }

  @Test
  void testS3Head_PassS3Client() throws IOException {
    try (var s3Client = getS3Client(getEmxSbCreds())) {
      // put a file
      var objectKey = "revloc02/source/test/test.txt";
      var metadata = Map.of("key", "value", "key2", "value2");
      var putObjectRequest =
          PutObjectRequest.builder().bucket(S3_INTERNAL).key(objectKey).metadata(metadata).build();
      s3Put(s3Client, getDefaultPayload(), putObjectRequest);

      // verify the file is there
      var objects = s3List(s3Client, S3_INTERNAL, objectKey);
      assertThat(objects.size()).isOne();
      assertThat(objects.get(0).key()).isEqualTo(objectKey);

      // retrieve object and check it
      var response = s3Get(s3Client, S3_INTERNAL, objectKey);
      String payload = new String(response.readAllBytes());
      assertThat(payload).isEqualTo(getDefaultPayload());
      response.close();

      // use head to verify metadata
      var headObjectResponse = s3Head(s3Client, S3_INTERNAL, objectKey);
      assertThat(headObjectResponse.metadata()).containsEntry("key", "value");
      assertThat(headObjectResponse.metadata()).containsEntry("key2", "value2");

      // delete the file
      s3Delete(s3Client, S3_INTERNAL, objectKey);

      // verify the file is gone
      objects = s3List(s3Client, S3_INTERNAL, objectKey);
      assertThat(objects).isEmpty();
    }
  }

  @Test
  void testS3Move_PassClient() throws IOException {
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      // place a file
      var objectKey = "revloc02/source/test/test.txt";
      var payload = getDefaultPayload();
      s3Put(s3Client, S3_INTERNAL, objectKey, payload);

      // verify the file is in the source
      var objects = s3List(s3Client, S3_INTERNAL, objectKey);
      assertThat(objects.size()).isOne();
      assertThat(objects.get(0).key()).isEqualTo(objectKey);

      // move file
      var destKey = "blake/inbound/dev/some-bank/ack/testCopied.txt";
      s3Move(s3Client, S3_INTERNAL, objectKey, S3_TARGET_CUSTOMER, destKey);

      // verify the file is in the new location
      objects = s3List(s3Client, S3_TARGET_CUSTOMER, destKey);
      assertThat(objects.size()).isOne();
      assertThat(objects.get(0).key()).isEqualTo(destKey);

      // check the contents
      var response = s3Get(s3Client, S3_TARGET_CUSTOMER, destKey);
      LOG.info("response={}", response.response());
      LOG.info("statusCode={}", response.response().sdkHttpResponse().statusCode());
      var respPayload = new String(response.readAllBytes(), StandardCharsets.UTF_8);
      assertThat(respPayload).isEqualTo(payload);

      // verify the file is no longer on the source s3
      objects = s3List(s3Client, S3_INTERNAL, objectKey);
      assertThat(objects).isEmpty();

      // cleanup and delete the files
      response.close();
      s3Delete(s3Client, S3_TARGET_CUSTOMER, destKey);
    }
  }

  @Test
  void testS3MoveAll_PassClient() {
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      LOG.info("...place several files...");
      var numFiles = 14; // don't change this to >1000, because s3List can only list 1000 items
      var keyPrefix = "revloc02/source/test/";
      for (var i = 0; i < numFiles; i++) {
        var objectKey = keyPrefix + "test-" + i + ".txt";
        var payload = getDefaultPayload() + " " + i;
        s3Put(s3Client, S3_INTERNAL, objectKey, payload);
      }

      LOG.info("...verify the files are on the source...");
      // if `numFiles` is >1000, await() won't work because `s3List` can only list 1000 files at a
      // time.
      await()
          .pollInterval(Duration.ofSeconds(3))
          .atMost(Duration.ofSeconds(60))
          .untilAsserted(
              () ->
                  assertThat(s3List(s3Client, S3_INTERNAL, keyPrefix, numFiles)).hasSize(numFiles));

      LOG.info("...move all files...");
      s3MoveAll(s3Client, S3_INTERNAL, keyPrefix, S3_TARGET_CUSTOMER);

      LOG.info("...verify the files are in the new location...");
      // if `numFiles` is >1000, await() won't work because `s3List` can only list 1000 files at a
      // time.
      await()
          .pollInterval(Duration.ofSeconds(3))
          .atMost(Duration.ofSeconds(60))
          .untilAsserted(
              () ->
                  assertThat(s3List(s3Client, S3_TARGET_CUSTOMER, keyPrefix, numFiles))
                      .hasSize(numFiles));

      LOG.info("...verify the source is empty...");
      await()
          .pollInterval(Duration.ofSeconds(3))
          .atMost(Duration.ofSeconds(60))
          .untilAsserted(() -> assertThat(s3List(s3Client, S3_INTERNAL, keyPrefix)).isEmpty());

      LOG.info("...cleanup and delete the files...");
      s3DeleteAll(s3Client, S3_TARGET_CUSTOMER, keyPrefix);
    }
  }

  @Test
  void testS3DeleteAll_PassClient() {
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      LOG.info("...place several files...");
      var numFiles = 24;
      var keyPrefix = "revloc02/target/test/";
      for (var i = 0; i < numFiles; i++) {
        var objectKey = keyPrefix + "test-" + i + ".txt";
        var payload = getDefaultPayload() + " " + i;
        s3Put(s3Client, S3_INTERNAL, objectKey, payload);
      }

      LOG.info("...verify the files are on the s3...");
      await()
          .pollInterval(Duration.ofSeconds(3))
          .atMost(Duration.ofSeconds(60))
          .untilAsserted(
              () ->
                  assertThat(s3List(s3Client, S3_INTERNAL, keyPrefix, numFiles)).hasSize(numFiles));

      LOG.info("...delete all of the files...");
      s3DeleteAll(s3Client, S3_INTERNAL, keyPrefix);

      LOG.info("...verify the s3 is empty...");
      await()
          .pollInterval(Duration.ofSeconds(3))
          .atMost(Duration.ofSeconds(60))
          .untilAsserted(() -> assertThat(s3List(s3Client, S3_INTERNAL, keyPrefix)).isEmpty());
    }
  }

  @Test
  void testS3CopyAll_PassClient() {
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      LOG.info("...place several files...");
      var numFiles = 14;
      var keyPrefix = "revloc02/source/test/";
      for (var i = 0; i < numFiles; i++) {
        var objectKey = keyPrefix + "test-" + i + ".txt";
        var payload = getDefaultPayload() + " " + i;
        s3Put(s3Client, S3_INTERNAL, objectKey, payload);
      }

      LOG.info("...verify the files are on the source...");
      await()
          .pollInterval(Duration.ofSeconds(3))
          .atMost(Duration.ofSeconds(60))
          .untilAsserted(
              () ->
                  assertThat(s3List(s3Client, S3_INTERNAL, keyPrefix, numFiles)).hasSize(numFiles));

      LOG.info("...copy all files...");
      s3CopyAll(s3Client, S3_INTERNAL, keyPrefix, S3_TARGET_CUSTOMER);

      LOG.info("...verify the files are in the new location...");
      await()
          .pollInterval(Duration.ofSeconds(3))
          .atMost(Duration.ofSeconds(60))
          .untilAsserted(
              () ->
                  assertThat(s3List(s3Client, S3_TARGET_CUSTOMER, keyPrefix, numFiles))
                      .hasSize(numFiles));

      LOG.info("...verify the source still contains all files...");
      await()
          .pollInterval(Duration.ofSeconds(3))
          .atMost(Duration.ofSeconds(60))
          .untilAsserted(
              () ->
                  assertThat(s3List(s3Client, S3_INTERNAL, keyPrefix, numFiles)).hasSize(numFiles));

      LOG.info("...cleanup and delete all files...");
      s3DeleteAll(s3Client, S3_INTERNAL, keyPrefix);
      s3DeleteAll(s3Client, S3_TARGET_CUSTOMER, keyPrefix);
    }
  }

  @Test
  void testS3CopyAllThousands() {
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      LOG.info("...place more than 1000 files...");
      var numFiles = 1100;
      var keyPrefix = "revloc02/source/test/";
      for (var i = 0; i < numFiles; i++) {
        var objectKey = keyPrefix + "test-" + i + ".txt";
        var payload = getDefaultPayload() + " " + i;
        s3Put(s3Client, S3_INTERNAL, objectKey, payload);
      }

      LOG.info("...verify the files are on the source...");
      await()
          .pollInterval(Duration.ofSeconds(3))
          .atMost(Duration.ofSeconds(60))
          .untilAsserted(
              () ->
                  assertThat(s3CountAll(s3Client, S3_INTERNAL, keyPrefix))
                      .isGreaterThanOrEqualTo(numFiles)); // sometimes there is 1 extra

      LOG.info("...copy all files...");
      s3CopyAll(s3Client, S3_INTERNAL, keyPrefix, S3_TARGET_CUSTOMER);

      LOG.info("...verify the files are in the new location...");
      await()
          .pollInterval(Duration.ofSeconds(3))
          .atMost(Duration.ofSeconds(60))
          .untilAsserted(
              () ->
                  assertThat(s3CountAll(s3Client, S3_TARGET_CUSTOMER, keyPrefix))
                      .isGreaterThanOrEqualTo(numFiles));

      LOG.info("...verify the source still contains all files...");
      await()
          .pollInterval(Duration.ofSeconds(3))
          .atMost(Duration.ofSeconds(60))
          .untilAsserted(
              () ->
                  assertThat(s3CountAll(s3Client, S3_INTERNAL, keyPrefix))
                      .isGreaterThanOrEqualTo(numFiles));

      LOG.info("...cleanup and delete all files...");
      s3DeleteAll(s3Client, S3_INTERNAL, keyPrefix);
      s3DeleteAll(s3Client, S3_TARGET_CUSTOMER, keyPrefix);
    }
  }

  /**
   * WARNING: This test leaves versioned objects on the bucket that are not cleaned up. Back when I
   * wrote this, putting more than 1000 files on this Internal S3 bucket was benign. But since then
   * object versioning has been turned on for this bucket, and now this test leaves behind a bunch
   * of versioned objects with delete markers.
   */
  @Test
  void testS3CountAll() {
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      LOG.info("...place several files...");
      var numFiles = 1234;
      var keyPrefix = "revloc02/source/test/";
      for (var i = 0; i < numFiles; i++) {
        var objectKey = keyPrefix + "test-" + i + ".txt";
        var payload = getDefaultPayload() + " " + i;
        s3Put(s3Client, S3_INTERNAL, objectKey, payload);
      }

      LOG.info("...verify the files are on the s3...");
      await()
          .pollInterval(Duration.ofSeconds(5))
          .atMost(Duration.ofSeconds(50))
          .untilAsserted(
              () -> assertThat(s3CountAll(s3Client, S3_INTERNAL, keyPrefix)).isEqualTo(numFiles));

      LOG.info("...cleanup and delete all files...");
      s3DeleteAll(s3Client, S3_INTERNAL, keyPrefix);
    }
  }

  @Test
  void testS3CountAllLessThanOneThousand() {
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      LOG.info("...place several files...");
      var numFiles = 23;
      var keyPrefix = "revloc02/source/test/";
      for (var i = 0; i < numFiles; i++) {
        var objectKey = keyPrefix + "test-" + i + ".txt";
        var payload = getDefaultPayload() + " " + i;
        s3Put(s3Client, S3_INTERNAL, objectKey, payload);
      }

      LOG.info("...verify the files are on the s3...");
      await()
          .pollInterval(Duration.ofSeconds(3))
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () ->
                  assertThat(s3CountAll(s3Client, S3_INTERNAL, keyPrefix))
                      .isGreaterThanOrEqualTo(numFiles)); // todo: why is there sometimes 24?

      LOG.info("...cleanup and delete all files...");
      s3DeleteAll(s3Client, S3_INTERNAL, keyPrefix);
    }
  }

  /** Tests S3 ListVersions. (Remember to get creds first using `aws configure sso`.) */
  @Test
  void testS3ListVersions() {
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      var keyPrefix = "revloc02/target/list-versions/";
      var objectKey = keyPrefix + "file-with-versions.txt";
      LOG.info("...place the same file 3 times...");
      s3Put(s3Client, S3_INTERNAL, objectKey, getDefaultPayload());
      s3Put(s3Client, S3_INTERNAL, objectKey, getDefaultPayload());
      s3Put(s3Client, S3_INTERNAL, objectKey, getDefaultPayload());

      LOG.info("...check that there is one file in a list...");
      var objects = s3List(creds, S3_INTERNAL, keyPrefix);
      assertThat(objects).hasSize(1);
      assertThat(objects.get(0).size()).isEqualTo(40L);

      LOG.info("...check for 3 versions...");
      var versions = s3ListVersions(s3Client, S3_INTERNAL, keyPrefix);
      assertThat(versions).hasSizeGreaterThanOrEqualTo(3);

      LOG.info("...cleanup and delete the file and its versions...");
      for (var version : versions) {
        S3Operations.s3DeleteVersion(s3Client, S3_INTERNAL, objectKey, version.versionId());
      }
      var deleteMarkers = s3ListDeleteMarkers(s3Client, S3_INTERNAL, keyPrefix);
      for (var deleteMarker : deleteMarkers) {
        S3Operations.s3DeleteVersion(s3Client, S3_INTERNAL, objectKey, deleteMarker.versionId());
      }
      LOG.info("...assert cleanup...");
      await()
          .pollInterval(Duration.ofSeconds(3))
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(
              () -> assertThat(s3ListVersions(s3Client, S3_INTERNAL, keyPrefix)).isEmpty());
    }
  }

  /**
   * Version cleanup method 1 (Recommended): Demonstrates cleaning up object versions by just
   * directly deleting all the version.versionId()s. If you specify a versionId in your delete
   * request, objects are deleted permanently and a delete marker is not created. (Remember to get
   * creds first using `aws configure sso`.)
   */
  @Test
  void testS3VersionCleanupMethod1() {
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      var keyPrefix = "revloc02/target/list-versions/";
      var objectKey = keyPrefix + "file-with-versions.txt";
      LOG.info("...place the same file 3 times...");
      s3Put(s3Client, S3_INTERNAL, objectKey, getDefaultPayload());
      s3Put(s3Client, S3_INTERNAL, objectKey, getDefaultPayload());
      s3Put(s3Client, S3_INTERNAL, objectKey, getDefaultPayload());

      LOG.info("...check that there is one file in a list...");
      var objects = s3List(creds, S3_INTERNAL, keyPrefix);
      assertThat(objects).hasSize(1);
      assertThat(objects.get(0).size()).isEqualTo(40L);

      LOG.info("...check for 3 versions...");
      var versions = s3ListVersions(s3Client, S3_INTERNAL, keyPrefix);
      assertThat(versions).hasSizeGreaterThanOrEqualTo(3);

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
   * Version cleanup method 2 (not recommended): Demonstrates cleaning up object versions by: 1)
   * deleting the object, then 2) deleting all of the versions of that object, and finally 3)
   * deleting the object delete-marker. (Remember to get creds first using `aws configure sso`.)
   */
  @Test
  void testS3VersionCleanupMethod2() {
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      var keyPrefix = "revloc02/target/list-versions/";
      var objectKey = keyPrefix + "file-with-versions.txt";
      LOG.info("...place the same file 3 times...");
      s3Put(s3Client, S3_INTERNAL, objectKey, getDefaultPayload());
      s3Put(s3Client, S3_INTERNAL, objectKey, getDefaultPayload());
      s3Put(s3Client, S3_INTERNAL, objectKey, getDefaultPayload());

      LOG.info("...check that there is one file in a list...");
      var objects = s3List(creds, S3_INTERNAL, keyPrefix);
      assertThat(objects).hasSize(1);
      assertThat(objects.get(0).size()).isEqualTo(40L);

      LOG.info("...check for 3 versions...");
      var versions = s3ListVersions(s3Client, S3_INTERNAL, keyPrefix);
      assertThat(versions).hasSizeGreaterThanOrEqualTo(3);

      LOG.info("...delete the object itself...");
      s3Delete(s3Client, S3_INTERNAL, keyPrefix + objectKey);
      LOG.info("...now delete all the versions of that object...");
      for (var version : versions) {
        S3Operations.s3DeleteVersion(s3Client, S3_INTERNAL, objectKey, version.versionId());
      }
      LOG.info("...cleanup and delete the delete markers...");
      var deleteMarkers = s3ListDeleteMarkers(s3Client, S3_INTERNAL, keyPrefix);
      for (var deleteMarker : deleteMarkers) {
        S3Operations.s3DeleteVersion(s3Client, S3_INTERNAL, objectKey, deleteMarker.versionId());
      }
      LOG.info("...assert cleanup...");
      await()
          .pollInterval(Duration.ofSeconds(3))
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(
              () -> assertThat(s3ListVersions(s3Client, S3_INTERNAL, keyPrefix)).isEmpty());
    }
  }

  /**
   * S3 wildcard search. Demonstrating that we can do a search on s3 for partial filenames.
   * (Remember to get creds first using `aws configure sso`.)
   */
  @Test
  void testS3WildcardSearch() {
    var creds = getEmxSbCreds();
    List<String> filenameList =
        List.of(
            "test1.txt",
            "test2.txt",
            "test3.txt",
            "test4.txt",
            "wildcard5.txt",
            "testwildcard6.txt");
    var count = 0;
    try (var s3Client = getS3Client(creds)) {
      var keyPrefix = "revloc02/target/contains";

      LOG.info("...place files...");
      for (var filename : filenameList) {
        var objectKey = keyPrefix + "/" + filename;
        s3Put(s3Client, S3_INTERNAL, objectKey, getDefaultPayload());
      }

      LOG.info("...check the files arrived...");
      await()
          .pollInterval(Duration.ofSeconds(3))
          .atMost(Duration.ofSeconds(60))
          .untilAsserted(
              () ->
                  assertThat(s3List(s3Client, S3_INTERNAL, keyPrefix, 10))
                      .hasSizeGreaterThanOrEqualTo(filenameList.size()));

      LOG.info("...find the files with 'wildcard' in the name...");
      var response = s3ListResponse(s3Client, S3_INTERNAL, keyPrefix, 1000);
      if (response.hasContents()) {
        LOG.info("response.contents().size(): {}", response.contents().size());
        for (var object : response.contents()) {
          if (object.key().contains("wildcard")) {
            LOG.info("object: {}", object.key());
            count++;
          }
        }
      }
      assertThat(count).isEqualTo(2);

      LOG.info("...cleanup and delete all version IDs...");
      var versions = s3ListVersions(s3Client, S3_INTERNAL, keyPrefix);
      for (var version : versions) {
        S3Operations.s3DeleteVersion(s3Client, S3_INTERNAL, version.key(), version.versionId());
      }
      LOG.info("...cleanup and delete the delete markers...");
      var deleteMarkers = s3ListDeleteMarkers(s3Client, S3_INTERNAL, keyPrefix);
      for (var deleteMarker : deleteMarkers) {
        S3Operations.s3DeleteVersion(
            s3Client, S3_INTERNAL, deleteMarker.key(), deleteMarker.versionId());
      }
      LOG.info("...assert cleanup...");
      await()
          .pollInterval(Duration.ofSeconds(3))
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(
              () -> assertThat(s3ListVersions(s3Client, S3_INTERNAL, keyPrefix)).isEmpty());
    }
    LOG.info("There was {} file with 'wildcard' in the name.", count);
  }

  /** Checks counting many, many thousands of objects. */
  @Test
  void countAllS3LoggingFiles() {
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      LOG.info(
          "sandbox-sftp-s3-logging-file-count={}",
          s3CountAll(s3Client, "cp-aws-gayedtiak3nflbiftucz-s3-logging", "emx-sandbox-sftp"));
    }
  }
}
