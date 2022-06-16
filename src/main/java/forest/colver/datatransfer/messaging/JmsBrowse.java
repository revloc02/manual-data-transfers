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

  public static void browseNextMessage(Environment env, String queueName) {
    var cf = new JmsConnectionFactory(env.url());
    try (var ctx = cf.createContext(getUsername(), getPassword())) {
      var q = ctx.createQueue(queueName);
      Enumeration msgs;
      try (var browser = ctx.createBrowser(q)) {
        msgs = browser.getEnumeration();
        var message = (Message) msgs.nextElement();
        LOG.info(
            "Next message BROWSED Host={}, Queue={}, Message->{}",
            env.name(),
            queueName,
            createStringFromMessage(message));
        var msgCount = Collections.list(msgs).size() + 1; // this empties msgs Enumeration series
        LOG.info("Queue={}:{}; MessageCountFromSelector={}\n", env.name(), queueName, msgCount);
      } catch (JMSException e) {
        e.printStackTrace();
      }
    }
  }

  public static void browseForSpecificMessage(Environment env, String queueName, String selector) {
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
        var msgCount = Collections.list(msgs).size() + 1; // this empties msgs Enumeration series
        LOG.info("Queue={}:{}; MessageCountFromSelector={}\n", env.name(), queueName, msgCount);
      } catch (JMSException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Browse a queue and count the messages that result from a specific message selector.
   *
   * @param env The environment.
   * @param queueName The queue.
   * @param selector The selector to identify specific messages.
   * @return The specific message count.
   */
  public static int browseAndCountSpecificMessages(Environment env, String queueName,
      String selector) {
    var cf = new JmsConnectionFactory(env.url());
    var msgCount = 0;
    try (var ctx = cf.createContext(getUsername(), getPassword())) {
      var q = ctx.createQueue(queueName);
      Enumeration msgs;
      try (var browser = ctx.createBrowser(q, selector)) {
        msgs = browser.getEnumeration();
        msgCount = Collections.list(msgs).size();
        LOG.info("Queue={}:{}; MessageCountFromSelector={}\n", env.name(), queueName, msgCount);
      } catch (JMSException e) {
        e.printStackTrace();
      }
    }
    return msgCount;
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
        LOG.info("Queue={}:{} Depth={}", env.name(), queueName, queueDepth);
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
              "Copied Message {}, sent to Queue={}:{}, Message->{}",
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
              "Copied Message {}, sent to Queue={}:{}, Message->{}",
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

  public static void copyAllMessagesAcrossEnvironments(Environment fromEnv, String fromQName,
      Environment toEnv, String toQName) {
    var fromCf = new JmsConnectionFactory(fromEnv.url());
    try (var fromCtx = fromCf.createContext(getUsername(), getPassword())) {
      var fromQ = fromCtx.createQueue(fromQName);
      var toQ = fromCtx.createQueue(toQName);
      Enumeration msgs;
      int count = 0;
      try (var browser = fromCtx.createBrowser(fromQ)) {
        msgs = browser.getEnumeration();
        var toCf = new JmsConnectionFactory(toEnv.url());
        try (var toCtx = toCf.createContext(getUsername(), getPassword())) {
          while (msgs.hasMoreElements()) {
            var message = (Message) msgs.nextElement();
            toCtx.createProducer().send(toQ, message);
            count++;
            LOG.info(
                "Copied Message {}, from Queue={}:{}, sent to Queue={}:{}, Message->{}",
                count,
                fromEnv.name(),
                fromQName,
                toEnv.name(),
                toQName,
                createStringFromMessage(message));
          }
          if (count == 0) {
            LOG.info("Could not find message to copy");
          }
        }
      } catch (JMSException e) {
        e.printStackTrace();
      }
      LOG.info("Copied {} messages. From Queue={}:{}, sent to Queue={}:{}", count,
          fromEnv.name(),
          fromQName,
          toEnv.name(),
          toQName);
    }
  }
}
