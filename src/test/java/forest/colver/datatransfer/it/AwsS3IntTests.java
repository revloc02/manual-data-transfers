package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.aws.S3Operations.s3Copy;
import static forest.colver.datatransfer.aws.S3Operations.s3Delete;
import static forest.colver.datatransfer.aws.S3Operations.s3Get;
import static forest.colver.datatransfer.aws.S3Operations.s3Head;
import static forest.colver.datatransfer.aws.S3Operations.s3List;
import static forest.colver.datatransfer.aws.S3Operations.s3Put;
import static forest.colver.datatransfer.aws.Utils.S3_INTERNAL;
import static forest.colver.datatransfer.aws.Utils.S3_TARGET_CUSTOMER;
import static forest.colver.datatransfer.aws.Utils.getEmxSbCreds;
import static forest.colver.datatransfer.aws.Utils.getS3Client;
import static forest.colver.datatransfer.config.Utils.getDefaultPayload;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Integration Tests for AWS S3
 */
public class AwsS3IntTests {

  // todo: see if I can use awaitlity on some of the asserts
  private static final Logger LOG = LoggerFactory.getLogger(AwsS3IntTests.class);

  // todo: so none of these tests check the payload of the arrived file, is that too hard or something? Figure it out, then fix it or comment on it so I don't look at this again sometime.

  // Pass parameters to the S3 operation

  /**
   * Each S3 operation uses the creds to create its own S3Client.
   */
  @Test
  public void testS3PutPassCredsAndParams() {
    // put a file
    var objectKey = "revloc02/source/test/test.txt";
    var creds = getEmxSbCreds();
    s3Put(creds, S3_INTERNAL, objectKey, getDefaultPayload());

    // verify the file is there
    var objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects.size()).isEqualTo(1);
    assertThat(objects.get(0).key()).isEqualTo(objectKey);
    // just testing the Put, so purposely not checking the payload value

    // delete the file
    s3Delete(creds, S3_INTERNAL, objectKey);

    // verify the file is gone
    objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects.size()).isEqualTo(0);
  }

  @Test
  public void testS3PutPassS3ClientAndParams() {
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      // put a file
      var objectKey = "revloc02/source/test/test.txt";
      s3Put(s3Client, S3_INTERNAL, objectKey, getDefaultPayload());

      // verify the file is there
      var objects = s3List(s3Client, S3_INTERNAL, objectKey);
      assertThat(objects.size()).isEqualTo(1);
      assertThat(objects.get(0).key()).isEqualTo(objectKey);
      // just testing the Put, so purposely not checking the payload value

      // delete the file
      s3Delete(s3Client, S3_INTERNAL, objectKey);

      // verify the file is gone
      objects = s3List(s3Client, S3_INTERNAL, objectKey);
      assertThat(objects.size()).isEqualTo(0);
    }
  }

  @Test
  public void testS3PutPassCredsAndParamsWithMetadata() {
    // put a file
    var objectKey = "revloc02/source/test/test.txt";
    var creds = getEmxSbCreds();
    var metadata = Map.of("key", "value", "key2", "value2");
    s3Put(creds, S3_INTERNAL, objectKey, getDefaultPayload(), metadata);

    // verify the file is there
    var objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects.size()).isEqualTo(1);
    assertThat(objects.get(0).key()).isEqualTo(objectKey);
    // just testing the Put, so purposely not checking the payload value

    // use head to verify metadata
    var headObjectResponse = s3Head(creds, S3_INTERNAL, objectKey);
    assertThat(headObjectResponse.metadata().get("key")).isEqualTo("value");
    assertThat(headObjectResponse.metadata().get("key2")).isEqualTo("value2");

    // delete the file
    s3Delete(creds, S3_INTERNAL, objectKey);

    // verify the file is gone
    objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects.size()).isEqualTo(0);
  }

  @Test
  public void testS3PutPassS3ClientAndParamsWithMetadata() {
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      // put a file
      var objectKey = "revloc02/source/test/test.txt";
      var metadata = Map.of("key", "value", "key2", "value2");
      s3Put(s3Client, S3_INTERNAL, objectKey, getDefaultPayload(), metadata);

      // verify the file is there
      var objects = s3List(s3Client, S3_INTERNAL, objectKey);
      assertThat(objects.size()).isEqualTo(1);
      assertThat(objects.get(0).key()).isEqualTo(objectKey);
      // just testing the Put, so purposely not checking the payload value

      // use head to verify metadata
      var headObjectResponse = s3Head(s3Client, S3_INTERNAL, objectKey);
      assertThat(headObjectResponse.metadata().get("key")).isEqualTo("value");
      assertThat(headObjectResponse.metadata().get("key2")).isEqualTo("value2");

      // delete the file
      s3Delete(s3Client, S3_INTERNAL, objectKey);

      // verify the file is gone
      objects = s3List(s3Client, S3_INTERNAL, objectKey);
      assertThat(objects.size()).isEqualTo(0);
    }
  }

  // Pass a PutObjectRequest to the S3 operation

  /**
   * Each S3 operation uses the creds to create its own S3Client.
   */
  @Test
  public void testS3PutPassCredsPassPutObjReq() {
    // put a file
    var objectKey = "revloc02/source/test/test.txt";
    var creds = getEmxSbCreds();
    var putObjectRequest = PutObjectRequest.builder()
        .bucket(S3_INTERNAL)
        .key(objectKey)
        .build();
    s3Put(creds, getDefaultPayload(), putObjectRequest);

    // verify the file is there
    var objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects.size()).isEqualTo(1);
    assertThat(objects.get(0).key()).isEqualTo(objectKey);
    // just testing the Put, so purposely not checking the payload value

    // delete the file
    s3Delete(creds, S3_INTERNAL, objectKey);

    // verify the file is gone
    objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects.size()).isEqualTo(0);
  }

  /**
   * One S3Client is created and then passed to each of the S3 operations.
   */
  @Test
  public void testS3PutPassClientPassPutObjReq() {
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      // put a file
      var objectKey = "revloc02/source/test/test.txt";
      var putObjectRequest = PutObjectRequest.builder()
          .bucket(S3_INTERNAL)
          .key(objectKey)
          .build();
      s3Put(s3Client, getDefaultPayload(), putObjectRequest);

      // verify the file is there
      var objects = s3List(s3Client, S3_INTERNAL, objectKey);
      assertThat(objects.size()).isEqualTo(1);
      assertThat(objects.get(0).key()).isEqualTo(objectKey);

      // delete the file
      s3Delete(s3Client, S3_INTERNAL, objectKey);

      // verify the file is gone
      objects = s3List(s3Client, S3_INTERNAL, objectKey);
      assertThat(objects.size()).isEqualTo(0);
    }
  }

  @Test
  public void testS3PutPassClientPassObjReqWithTagging() throws IOException {
    try (var s3Client = getS3Client(getEmxSbCreds())) {
      // put a file
      var objectKey = "revloc02/source/test/test.txt";
      var tagging = "expirableObject=true";
      var putObjectRequest = PutObjectRequest.builder()
          .bucket(S3_INTERNAL)
          .key(objectKey)
          .tagging(tagging)
          .build();
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
      assertThat(objects.size()).isEqualTo(0);
    }
  }

  @Test
  public void testS3HeadPassCredsPassPutObjReqWithMetadata() {
    // put a file
    var objectKey = "revloc02/source/test/test.txt";
    var creds = getEmxSbCreds();
    var metadata = Map.of("key", "value");
    var putObjectRequest = PutObjectRequest.builder()
        .bucket(S3_INTERNAL)
        .key(objectKey)
        .metadata(metadata)
        .build();
    s3Put(creds, getDefaultPayload(), putObjectRequest);

    // verify the file is there
    var objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects.size()).isEqualTo(1);
    assertThat(objects.get(0).key()).isEqualTo(objectKey);

    // use head to verify metadata
    var headObjectResponse = s3Head(creds, S3_INTERNAL, objectKey);
    assertThat(headObjectResponse.metadata().get("key")).isEqualTo("value");

    // delete the file
    s3Delete(creds, S3_INTERNAL, objectKey);

    // verify the file is gone
    objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects.size()).isEqualTo(0);
  }

  @Test
  public void testS3HeadPassS3Client() throws IOException {
    try (var s3Client = getS3Client(getEmxSbCreds())) {
      // put a file
      var objectKey = "revloc02/source/test/test.txt";
      var metadata = Map.of("key", "value", "key2", "value2");
      var putObjectRequest = PutObjectRequest.builder()
          .bucket(S3_INTERNAL)
          .key(objectKey)
          .metadata(metadata)
          .build();
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
      assertThat(headObjectResponse.metadata().get("key")).isEqualTo("value");
      assertThat(headObjectResponse.metadata().get("key2")).isEqualTo("value2");

      // delete the file
      s3Delete(s3Client, S3_INTERNAL, objectKey);

      // verify the file is gone
      objects = s3List(s3Client, S3_INTERNAL, objectKey);
      assertThat(objects.size()).isEqualTo(0);
    }
  }

  // todo: either move these up, or figure out a better organization, or forget organization
  // From here down are test that pass params (not PutObjectRequest)

  @Test
  public void testS3CopyPassClient() throws IOException {
    var creds = getEmxSbCreds();
    try (var s3Client = getS3Client(creds)) {
      // place a file
      var objectKey = "revloc02/source/test/test.txt";
      var payload = getDefaultPayload();
      s3Put(s3Client, S3_INTERNAL, objectKey, payload);

      // verify the file is in the source
      var objects = s3List(s3Client, S3_INTERNAL, objectKey);
      assertThat(objects.size()).isEqualTo(1);
      assertThat(objects.get(0).key()).isEqualTo(objectKey);

      // copy file
      var destKey = "blake/inbound/dev/some-bank/ack/testCopied.txt";
      s3Copy(s3Client, S3_INTERNAL, objectKey, S3_TARGET_CUSTOMER, destKey);

      // verify the copy is in the new location
      objects = s3List(s3Client, S3_TARGET_CUSTOMER, destKey);
      assertThat(objects.size()).isEqualTo(1);
      assertThat(objects.get(0).key()).isEqualTo(destKey);

      // check the contents
      var response = s3Get(s3Client, S3_TARGET_CUSTOMER, destKey);
      LOG.info("response={}", response.response());
      LOG.info("statusCode={}", response.response().sdkHttpResponse().statusCode());
      var respPayload = new String(response.readAllBytes(),
          StandardCharsets.UTF_8);
      assertThat(respPayload).isEqualTo(payload);

      // cleanup and delete the files
      response.close();
      s3Delete(s3Client, S3_INTERNAL, objectKey);
      s3Delete(s3Client, S3_TARGET_CUSTOMER, destKey);
    }
  }

  @Test
  public void testS3Delete() {
    // put a file
    var objectKey = "revloc02/source/test/test.txt";
    var creds = getEmxSbCreds();
    s3Put(creds, S3_INTERNAL, objectKey, getDefaultPayload());

    // verify the file is there
    var objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects.size()).isEqualTo(1);
    assertThat(objects.get(0).key()).isEqualTo(objectKey);

    // delete the file
    s3Delete(creds, S3_INTERNAL, objectKey);

    // verify the file is gone
    objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects.size()).isEqualTo(0);
  }

  @Test
  public void testS3List() {
    var creds = getEmxSbCreds();
    var objectKey = "revloc02/target/test/mdtTest1.txt";
    s3Put(creds, S3_INTERNAL, objectKey, getDefaultPayload());
    var objects = s3List(creds, S3_INTERNAL, "revloc02/target/test");
    assertThat(objects.get(1).key()).isEqualTo(objectKey);
    assertThat(objects.get(1).size()).isEqualTo(40L);
    s3Delete(creds, S3_INTERNAL, objectKey);
  }
}
