package forest.colver.datatransfer;

import static forest.colver.datatransfer.config.Utils.deleteFile;
import static forest.colver.datatransfer.config.Utils.getDefaultPayload;
import static forest.colver.datatransfer.config.Utils.readFile;
import static forest.colver.datatransfer.config.Utils.writeFile;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for config Utils.
 */
public class UtilsTests {

  @Test
  public void testWriteAndReadFile() {
    var path = "/Users/revloc02/Downloads/manual-data-transfers-test.txt";
    var payload = getDefaultPayload().getBytes(StandardCharsets.UTF_8);
    writeFile(path, payload);
    var contents = readFile(path, StandardCharsets.UTF_8);
    assertThat(contents).contains(getDefaultPayload());
    // clean up
    deleteFile(path);
  }
}
