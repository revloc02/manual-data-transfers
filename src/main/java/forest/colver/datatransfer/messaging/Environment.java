package forest.colver.datatransfer.messaging;

import static forest.colver.datatransfer.config.Utils.QPID_DEV;
import static forest.colver.datatransfer.config.Utils.QPID_PROD;
import static forest.colver.datatransfer.config.Utils.QPID_STAGE;
import static forest.colver.datatransfer.config.Utils.QPID_TEST;
import static forest.colver.datatransfer.config.Utils.TQ_PROD;
import static forest.colver.datatransfer.config.Utils.TQ_STAGE;

public enum Environment {
  DEV(QPID_DEV),
  TEST(QPID_TEST),
  STAGE(QPID_STAGE),
  PROD(QPID_PROD),
  TQSTAGE(TQ_STAGE),
  TQPROD(TQ_PROD),
  LOCALHOST("amqp://localhost:5672");

  private String url;

  Environment(String url) {
    this.url = url;
  }

  public String url() {
    return url;
  }
}