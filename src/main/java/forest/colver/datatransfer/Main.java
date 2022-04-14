package forest.colver.datatransfer;

import static forest.colver.datatransfer.messaging.DisplayUtils.*;
import static forest.colver.datatransfer.messaging.Environment.PROD;
import static forest.colver.datatransfer.messaging.Environment.STAGE;
import static forest.colver.datatransfer.messaging.JmsBrowse.*;
import static forest.colver.datatransfer.messaging.JmsConsume.*;
import static forest.colver.datatransfer.messaging.JmsSend.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final Logger LOG = LoggerFactory.getLogger(Main.class);
  public static final String EMX_ERROR = "emx-error";

  public static void main(String[] args) {
    LOG.info("start");

    listenForMessages(STAGE, "forest-test");

  }
}
