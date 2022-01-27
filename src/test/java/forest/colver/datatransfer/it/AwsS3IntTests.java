package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.aws.S3Operations.s3Copy;
import static forest.colver.datatransfer.aws.S3Operations.s3Delete;
import static forest.colver.datatransfer.aws.S3Operations.s3List;
import static forest.colver.datatransfer.aws.S3Operations.s3Put;
import static forest.colver.datatransfer.aws.Utils.getS3TargetCustomer;
import static forest.colver.datatransfer.aws.Utils.getSbCreds;
import static forest.colver.datatransfer.aws.Utils.getS3Internal;
import static forest.colver.datatransfer.config.Utils.getDefaultPayload;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration Tests for AWS S3
 */
public class AwsS3IntTests {

  private static final Logger LOG = LoggerFactory.getLogger(AwsS3IntTests.class);
  private static final String SANDBOX_SFTP_INTERNAL_BUCKET = getS3Internal();
  private static final String SANDBOX_SFTP_TARGET_CUSTOMER_BUCKET = getS3TargetCustomer();

  @Test
  public void testS3Copy() {
    // place a file
    var creds = getSbCreds();
    var sourceKey = "revloc02/source/test/test.txt";
    s3Put(creds, SANDBOX_SFTP_INTERNAL_BUCKET, sourceKey, getDefaultPayload());

    // verify the file is in the source
    var objects = s3List(creds, SANDBOX_SFTP_INTERNAL_BUCKET, sourceKey);
    assertThat(objects.size()).isEqualTo(1);
    assertThat(objects.get(0).key()).isEqualTo(sourceKey);

    // copy file
    var destKey = "blake/inbound/dev/some-bank/ack/testCopied.txt";
    s3Copy(creds, SANDBOX_SFTP_INTERNAL_BUCKET, sourceKey, SANDBOX_SFTP_TARGET_CUSTOMER_BUCKET, destKey);

    // verify the copy
    objects = s3List(creds, SANDBOX_SFTP_TARGET_CUSTOMER_BUCKET, destKey);
    assertThat(objects.size()).isEqualTo(1);
    assertThat(objects.get(0).key()).isEqualTo(destKey);

    // check the contents
//    s3Get(creds, SANDBOX_SFTP_TARGET_CUSTOMER_BUCKET, destKey);

    // delete the files
    s3Delete(creds, SANDBOX_SFTP_INTERNAL_BUCKET, sourceKey);
    s3Delete(creds, SANDBOX_SFTP_TARGET_CUSTOMER_BUCKET, destKey);
  }

  @Test
  public void testS3Delete() {
    // put a file
    var sourceKey = "revloc02/source/test/test.txt";
    var creds = getSbCreds();
    s3Put(creds, SANDBOX_SFTP_INTERNAL_BUCKET, sourceKey, getDefaultPayload());

    // verify the file is there
    var objects = s3List(creds, SANDBOX_SFTP_INTERNAL_BUCKET, sourceKey);
    assertThat(objects.size()).isEqualTo(1);
    assertThat(objects.get(0).key()).isEqualTo(sourceKey);

    // delete the file
    s3Delete(creds, SANDBOX_SFTP_INTERNAL_BUCKET, sourceKey);

    // verify the file is gone
    objects = s3List(creds, SANDBOX_SFTP_INTERNAL_BUCKET, sourceKey);
    assertThat(objects.size()).isEqualTo(0);
  }

  @Test
  public void testS3List() {
    var creds = getSbCreds();
    var key = "revloc02/target/test/mdtTest1.txt";
    s3Put(creds, SANDBOX_SFTP_INTERNAL_BUCKET, key, getDefaultPayload());
    var objects = s3List(creds, SANDBOX_SFTP_INTERNAL_BUCKET, "revloc02/target/test");
    assertThat(objects.get(1).key()).isEqualTo(key);
    assertThat(objects.get(1).size()).isEqualTo(40L);
    s3Delete(creds, SANDBOX_SFTP_INTERNAL_BUCKET, key);
  }
}
