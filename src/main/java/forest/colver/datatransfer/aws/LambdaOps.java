package forest.colver.datatransfer.aws;

import static forest.colver.datatransfer.aws.Utils.getLambdaClient;

import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

public class LambdaOps {

  private static final Logger LOG = LoggerFactory.getLogger(LambdaOps.class);

  public static InvokeResponse lambdaInvoke(AwsCredentialsProvider awsCp, String funcName,
      String p) {
    var payload = SdkBytes.fromUtf8String(p);
    try (var lambdaClient = getLambdaClient(awsCp)) {
      var invokeRequest = InvokeRequest.builder().functionName(funcName).payload(payload).build();
      LOG.info("Invoked {}", funcName);
      var response = lambdaClient.invoke(invokeRequest);
      LOG.info("Response {}: {}\n", response.statusCode(),
          response.payload().asString(StandardCharsets.UTF_8));
      return response;
    }
  }
}

