package forest.colver.datatransfer.messaging;

import static forest.colver.datatransfer.config.Utils.getPassword;
import static forest.colver.datatransfer.config.Utils.getUsername;
import static forest.colver.datatransfer.messaging.DisplayUtils.createStringFromMessage;
import static javax.jms.JMSContext.CLIENT_ACKNOWLEDGE;

import javax.jms.JMSException;
import javax.jms.Message;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsConsume {

  private static final Logger LOG = LoggerFactory.getLogger(JmsConsume.class);

  // todo: yeah so, this doesn't seem to work yet
  public static void deleteSpecificMessages(Environment env, String queueName, String selector) {
    var factory = new JmsConnectionFactory(getUsername(), getPassword(), env.url());
    try (var context = factory.createContext()) {
      var queue = context.createQueue(queueName);
      LOG.info("consume from {}, with selector {}", queueName, selector);
      final int[] deleteCounter = {0};
      try (var consumer = context.createConsumer(queue, selector)) {
        consumer.setMessageListener(
            message -> {
              deleteCounter[0]++;
              LOG.info("Deleted Message {}", deleteCounter[0]);
            });
      }
    }
  }

  /**
   * Deletes all messages from the queue.
   *
   * @param env The broker environment.
   * @param queueName Queue name.
   * @return The number of messages that were deleted from the queue.
   */
  public static int deleteAllMessagesFromQueue(Environment env, String queueName) {
    var cf = new JmsConnectionFactory(env.url());
    var counter = 0;
    try (var ctx = cf.createContext(getUsername(), getPassword())) {
      var queue = ctx.createQueue(queueName);
      try (var consumer = ctx.createConsumer(queue)) {
        Message message;
        do {
          message = consumer.receive(3_000);
          if (message != null) {
            counter++;
            message.acknowledge();
          }

        } while (message != null);
      } catch (JMSException e) {
        e.printStackTrace();
      }
    }
    LOG.info("Purged {} messages from {} queue.", counter, queueName);
    return counter;
  }

  /**
   * Purges all message from the queue. This method tends to hang if there is over 1000 messages to
   * purge, therefore see {@link #deleteAllMessagesFromQueue(Environment, String)}.
   *
   * @param env The broker environment.
   * @param queueName Queue name.
   * @return The number of messages that were purged from the queue.
   */
  public static int purgeQueue(Environment env, String queueName) {
    var cf = new JmsConnectionFactory(env.url());
    var counter = 0;
    try (var ctx = cf.createContext(getUsername(), getPassword())) {
      var queue = ctx.createQueue(queueName);
      try (var consumer = ctx.createConsumer(queue)) {
        Message message;
        do {
          message = consumer.receiveNoWait();
          if (message != null) {
            counter++;
            message.acknowledge();
          }

        } while (message != null);
      } catch (JMSException e) {
        e.printStackTrace();
      }
    }
    LOG.info("Purged {} messages from {} queue.", counter, queueName);
    return counter;
  }

  public static int deleteAllSpecificMessagesFromQueue(Environment env, String queueName,
      String selector) {
    var cf = new JmsConnectionFactory(env.url());
    var counter = 0;
    try (var ctx = cf.createContext(getUsername(), getPassword())) {
      var queue = ctx.createQueue(queueName);
      try (var consumer = ctx.createConsumer(queue, selector)) {
        Message message;
        do {
          message = consumer.receiveNoWait();
          if (message != null) {
            counter++;
            message.acknowledge();
          }

        } while (message != null);
      } catch (JMSException e) {
        e.printStackTrace();
      }
    }
    LOG.info("Purged {} messages from {} queue in {}.", counter, queueName, env.name());
    return counter;
  }

  // todo: i'd like to work on this one some more, I seem to get timeouts
  public static void deleteSomeMessagesFromQueue(Environment env, String queueName, int amount) {
    var cf = new JmsConnectionFactory(env.url());
    var counter = 0;
    try (var ctx = cf.createContext(getUsername(), getPassword())) {
      var queue = ctx.createQueue(queueName);
      try (var consumer = ctx.createConsumer(queue)) {
        Message message;
        for (var i = 0; i < amount; i++) {
          message = consumer.receiveNoWait();
          if (message != null) {
            message.acknowledge(); // todo: is this currently AUTO_ACK? yes. so is that causing the timeouts?
            counter++;
          }
        }
      } catch (JMSException e) {
        e.printStackTrace();
      }
    }
    LOG.info("Purged {} messages from {} queue.", counter, queueName);
  }

  // todo: what is this method supposed to do? It doesn't look like it is being used. Figure it out and write a Javadoc.
  public static void listenForMessages(Environment env, String queueName) {
    LOG.info("Listening for messages on {} from {} queue.", env.name(), queueName);
    var cf = new JmsConnectionFactory(env.url());
    try (var ctx = cf.createContext(getUsername(), getPassword())) {
      ctx.createConsumer(ctx.createQueue(queueName))
          .setMessageListener(
              message -> {
                createStringFromMessage(message);
              });
    }
  }

  public static Message consumeSpecificMessage(
      Environment env, String fromQueueName, String selector) {
    Message message = null;
    var cf = new JmsConnectionFactory(env.url());
    try (var ctx = cf.createContext(getUsername(), getPassword(), CLIENT_ACKNOWLEDGE)) {
      var fromQ = ctx.createQueue(fromQueueName);
      try (var consumer = ctx.createConsumer(fromQ, selector)) {
        message = consumer.receive(5_000L);
        LOG.info(
            "Consumed from Host={} Queue={}, Message->{}",
            env.name(),
            fromQueueName,
            createStringFromMessage(message));
        message.acknowledge();
      } catch (JMSException e) {
        e.printStackTrace();
      }
    }
    return message;
  }

  public static Message consumeOneMessage(Environment env, String fromQueueName) {
    Message message = null;
    var cf = new JmsConnectionFactory(env.url());
    try (var ctx = cf.createContext(getUsername(), getPassword(), CLIENT_ACKNOWLEDGE)) {
      var fromQ = ctx.createQueue(fromQueueName);
      try (var consumer = ctx.createConsumer(fromQ)) {
        message = consumer.receive(5_000L);
        message.acknowledge();
      } catch (JMSException e) {
        e.printStackTrace();
      }
    }
    return message;
  }

  public static void moveSpecificMessage(
      Environment env, String fromQueueName, String selector, String toQueueName) {
    var cf = new JmsConnectionFactory(env.url());
    try (var ctx = cf.createContext(getUsername(), getPassword(), CLIENT_ACKNOWLEDGE)) {
      var fromQ = ctx.createQueue(fromQueueName);
      try (var consumer = ctx.createConsumer(fromQ, selector)) {
        var message = consumer.receive(5_000L);
        var toQ = ctx.createQueue(toQueueName);
        ctx.createProducer().send(toQ, message);
        message.acknowledge();
        LOG.info(
            "Moved from Host={} Queue={} to Queue={}, Message->{}",
            env.name(),
            fromQueueName,
            toQueueName,
            createStringFromMessage(message));
      } catch (JMSException e) {
        e.printStackTrace();
      }
    }
    LOG.info("Done.");
  }

  public static void moveOneMessage(Environment env, String fromQueueName, String toQueueName) {
    var cf = new JmsConnectionFactory(env.url());
    try (var ctx = cf.createContext(getUsername(), getPassword(), CLIENT_ACKNOWLEDGE)) {
      var fromQ = ctx.createQueue(fromQueueName);
      try (var consumer = ctx.createConsumer(fromQ)) {
        var message = consumer.receive(5_000L);
        var toQ = ctx.createQueue(toQueueName);
        ctx.createProducer().send(toQ, message);
        message.acknowledge();
        LOG.info(
            "Moved from Host={} Queue={} to Queue={}, Message->{}",
            env.name(),
            fromQueueName,
            toQueueName,
            createStringFromMessage(message));
      } catch (JMSException e) {
        e.printStackTrace();
      }
    }
    LOG.info("Done.");
  }

  public static void moveAllMessages(Environment env, String fromQueueName, String toQueueName) {
    var cf = new JmsConnectionFactory(env.url());
    try (var ctx = cf.createContext(getUsername(), getPassword(), CLIENT_ACKNOWLEDGE)) {
      var fromQ = ctx.createQueue(fromQueueName);
      var counter = 0;
      try (var consumer = ctx.createConsumer(fromQ)) {
        var moreMessages = true;
        var toQ = ctx.createQueue(toQueueName);
        Message message;
        while (moreMessages) {
          message = consumer.receive(2_000L);
          if (message != null) {
            counter++;
            ctx.createProducer().send(toQ, message);
            message.acknowledge();
            LOG.info(
                "Moved from Host={} Queue={} to Queue={}, counter={}",
                env.name(),
                fromQueueName,
                toQueueName,
                counter);
          } else {
            moreMessages = false;
          }
        }
      } catch (JMSException e) {
        e.printStackTrace();
      }
      LOG.info("Moved {} messages.", counter);
    }
  }

  public static void moveAllSpecificMessages(
      Environment env, String fromQueueName, String selector, String toQueueName) {
    var cf = new JmsConnectionFactory(env.url());
    try (var ctx = cf.createContext(getUsername(), getPassword(), CLIENT_ACKNOWLEDGE)) {
      var fromQ = ctx.createQueue(fromQueueName);
      var counter = 0;
      try (var consumer = ctx.createConsumer(fromQ, selector)) {
        var moreMessages = true;
        var toQ = ctx.createQueue(toQueueName);
        Message message;
        while (moreMessages) {
          message = consumer.receive(2_000L);
          if (message != null) {
            counter++;
            ctx.createProducer().send(toQ, message);
            message.acknowledge();
            LOG.info(
                "Moved from Host={} Queue={} to Queue={}, counter={}",
                env.name(),
                fromQueueName,
                toQueueName,
                counter);
          } else {
            moreMessages = false;
          }
        }
      } catch (JMSException e) {
        e.printStackTrace();
      }
      LOG.info("Moved {} messages for selector={}.", counter, selector);
    }
  }

  public static void moveSomeSpecificMessages(
      Environment env, String fromQueueName, String selector, String toQueueName, int amount) {
    var cf = new JmsConnectionFactory(env.url());
    try (var ctx = cf.createContext(getUsername(), getPassword(), CLIENT_ACKNOWLEDGE)) {
      var fromQ = ctx.createQueue(fromQueueName);
      var counter = 0;
      try (var consumer = ctx.createConsumer(fromQ, selector)) {
        var toQ = ctx.createQueue(toQueueName);
        Message message;
        for (var i = 0; i < amount; i++) {
          message = consumer.receive(2_000L);
          if (message != null) {
            counter++;
            ctx.createProducer().send(toQ, message);
            message.acknowledge();
          }
        }
      } catch (JMSException e) {
        e.printStackTrace();
      }
      LOG.info("Moved {} messages from {} to {}, for selector={}.", counter, fromQueueName,
          toQueueName, selector);
    }
  }
}

