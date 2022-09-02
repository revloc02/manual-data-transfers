package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.aws.S3Operations.s3Copy;
import static forest.colver.datatransfer.aws.S3Operations.s3Delete;
import static forest.colver.datatransfer.aws.S3Operations.s3Head;
import static forest.colver.datatransfer.aws.S3Operations.s3List;
import static forest.colver.datatransfer.aws.S3Operations.s3Put;
import static forest.colver.datatransfer.aws.Utils.S3_INTERNAL;
import static forest.colver.datatransfer.aws.Utils.S3_TARGET_CUSTOMER;
import static forest.colver.datatransfer.aws.Utils.getEmxSbCreds;
import static forest.colver.datatransfer.config.Utils.getDefaultPayload;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Integration Tests for AWS S3
 */
public class AwsS3IntTests {

  private static final Logger LOG = LoggerFactory.getLogger(AwsS3IntTests.class);

  @Test
  public void testS3Copy() {
    // place a file
    var creds = getEmxSbCreds();
    var objectKey = "revloc02/source/test/test.txt";
    s3Put(creds, S3_INTERNAL, objectKey, getDefaultPayload());

    // verify the file is in the source
    var objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects.size()).isEqualTo(1);
    assertThat(objects.get(0).key()).isEqualTo(objectKey);

    // copy file
    var destKey = "blake/inbound/dev/some-bank/ack/testCopied.txt";
    s3Copy(creds, S3_INTERNAL, objectKey, S3_TARGET_CUSTOMER, destKey);

    // verify the copy
    objects = s3List(creds, S3_TARGET_CUSTOMER, destKey);
    assertThat(objects.size()).isEqualTo(1);
    assertThat(objects.get(0).key()).isEqualTo(destKey);

    // check the contents
//    s3Get(creds, SANDBOX_SFTP_TARGET_CUSTOMER_BUCKET, destKey);

    // delete the files
    s3Delete(creds, S3_INTERNAL, objectKey);
    s3Delete(creds, S3_TARGET_CUSTOMER, destKey);
  }

  @Test
  public void testS3PutObjectRequest() {
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

    // delete the file
    s3Delete(creds, S3_INTERNAL, objectKey);

    // verify the file is gone
    objects = s3List(creds, S3_INTERNAL, objectKey);
    assertThat(objects.size()).isEqualTo(0);
  }

  @Test
  public void testS3Head() {
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
