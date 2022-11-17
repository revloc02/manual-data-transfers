package forest.colver.datatransfer;

import static forest.colver.datatransfer.aws.Utils.convertSqsMessageAttributesToStrings;
import static forest.colver.datatransfer.aws.Utils.createSqsMessageAttributes;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

public class AwsUtilsTests {

  @Test
  public void testConvertMessageAttributesToStrings() {
    final Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
    messageAttributes.put("key1",
        MessageAttributeValue.builder().dataType("String").stringValue("value1").build());
    messageAttributes.put("key2",
        MessageAttributeValue.builder().dataType("String").stringValue("value2").build());
    var response = convertSqsMessageAttributesToStrings(messageAttributes);
    assertThat(response.get("key1")).isEqualTo("value1");
    assertThat(response.get("key2")).isEqualTo("value2");
  }

  @Test
  public void testCreateMessageAttributes() {
    var map = Map.of("key3", "value3", "key4", "value4");
    var response = createSqsMessageAttributes(map);
    assertThat(response.get("key3").stringValue()).isEqualTo("value3");
    assertThat(response.get("key4").stringValue()).isEqualTo("value4");
  }
}
