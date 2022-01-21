package forest.colver.datatransfer.messaging;

import static forest.colver.datatransfer.config.Utils.getPassword;
import static forest.colver.datatransfer.config.Utils.getUsername;
import static forest.colver.datatransfer.messaging.DisplayUtils.createStringFromMessage;

import java.util.Collections;
import java.util.Enumeration;
import javax.jms.JMSException;
import javax.jms.Message;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsBrowse {

  private static final Logger LOG = LoggerFactory.getLogger(JmsBrowse.class);

  public static void browseForMessage(Environment env, String queueName, String selector) {
    var cf = new JmsConnectionFactory(env.url());
    try (var ctx = cf.createContext(getUsername(), getPassword())) {
      var q = ctx.createQueue(queueName);
      Enumeration msgs;
      try (var browser = ctx.createBrowser(q, selector)) {
        msgs = browser.getEnumeration();
        var message = (Message) msgs.nextElement();
        LOG.info(
            "Next message BROWSED Host={}, Queue={}, Message->{}",
            env.name(),
            queueName,
            createStringFromMessage(message));
        var queueDepth = Collections.list(msgs).size(); // this empties msgs Enumeration series
        LOG.info("Queue {} Depth={}", queueName, queueDepth);
      } catch (JMSException e) {
        e.printStackTrace();
      }
    }
  }

  public static int queueDepth(Environment env, String queueName) {
    var cf = new JmsConnectionFactory(env.url());
    var queueDepth = 0;
    try (var ctx = cf.createContext(getUsername(), getPassword())) {
      var q = ctx.createQueue(queueName);
      Enumeration msgs;
      try (var browser = ctx.createBrowser(q)) {
        msgs = browser.getEnumeration();
        queueDepth = Collections.list(msgs).size();
        LOG.info("Queue {} Depth={}", queueName, queueDepth);
      } catch (JMSException e) {
        e.printStackTrace();
      }
    }
    return queueDepth;
  }

  public static void copySpecificMessages(
      Environment env, String fromQName, String selector, String toQName) {
    var cf = new JmsConnectionFactory(env.url());
    try (var ctx = cf.createContext(getUsername(), getPassword())) {
      var fromQ = ctx.createQueue(fromQName);
      var toQ = ctx.createQueue(toQName);
      Enumeration msgs;
      int count = 0;
      try (var browser = ctx.createBrowser(fromQ, selector)) {
        msgs = browser.getEnumeration();
        while (msgs.hasMoreElements()) {
          var message = (Message) msgs.nextElement();
          ctx.createProducer().send(toQ, message);
          count++;
          LOG.info(
              "Copied Message {}, sent to Host={}, Queue={}, Message->{}",
              count,
              env.name(),
              toQName,
              createStringFromMessage(message));
        }
        if (count == 0) {
          LOG.info("Could not find message to copy");
        }
      } catch (JMSException e) {
        e.printStackTrace();
      }
      LOG.info("Copied {} messages.", count);
    }
  }

  public static void copyAllMessages(Environment env, String fromQName, String toQName) {
    var cf = new JmsConnectionFactory(env.url());
    try (var ctx = cf.createContext(getUsername(), getPassword())) {
      var fromQ = ctx.createQueue(fromQName);
      var toQ = ctx.createQueue(toQName);
      Enumeration msgs;
      int count = 0;
      try (var browser = ctx.createBrowser(fromQ)) {
        msgs = browser.getEnumeration();
        while (msgs.hasMoreElements()) {
          var message = (Message) msgs.nextElement();
          ctx.createProducer().send(toQ, message);
          count++;
          LOG.info(
              "Copied Message {}, sent to Host={}, Queue={}, Message->{}",
              count,
              env.name(),
              toQName,
              createStringFromMessage(message));
        }
        if (count == 0) {
          LOG.info("Could not find message to copy");
        }
      } catch (JMSException e) {
        e.printStackTrace();
      }
      LOG.info("Copied {} messages.", count);
    }
  }
}
