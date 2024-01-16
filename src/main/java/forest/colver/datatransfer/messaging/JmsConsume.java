package forest.colver.datatransfer.messaging;

import static forest.colver.datatransfer.config.Utils.getPassword;
import static forest.colver.datatransfer.config.Utils.getUsername;
import static forest.colver.datatransfer.messaging.DisplayUtils.createStringFromMessage;
import static jakarta.jms.JMSContext.CLIENT_ACKNOWLEDGE;

import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSException;
import jakarta.jms.JMSProducer;
import jakarta.jms.Message;
import jakarta.jms.Queue;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsConsume {

  private JmsConsume(){
    // https://rules.sonarsource.com/java/RSPEC-1118/
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated.");
  }

  private static final Logger LOG = LoggerFactory.getLogger(JmsConsume.class);

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
    try (var ctx = cf.createContext(getUsername(), getPassword(), CLIENT_ACKNOWLEDGE)) {
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
    LOG.info("Deleted {} messages from {}:{} queue.", counter, env.name(), queueName);
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
    try (var ctx = cf.createContext(getUsername(), getPassword(), CLIENT_ACKNOWLEDGE)) {
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
    LOG.info("Purged {} messages from {}:{} queue.", counter, env.name(), queueName);
    return counter;
  }

  public static int deleteAllSpecificMessages(Environment env, String queueName,
      String selector) {
    var cf = new JmsConnectionFactory(env.url());
    var counter = 0;
    try (var ctx = cf.createContext(getUsername(), getPassword(), CLIENT_ACKNOWLEDGE)) {
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
    LOG.info("Deleted {} messages from {}:{} queue with criteria {}.", counter, env.name(), queueName, selector);
    return counter;
  }

  /**
   * Deletes the amount of messages, requested in the argument, from the queue. This exists
   * because...yeah, I don't remember. But I think it used to be that Qpid would get bogged down
   * when there were too many messages on the broker, and this was an effort to remove them chunks
   * at a time. Not sure if this has other practical uses.
   */
  public static void deleteSomeMessages(Environment env, String queueName, int amount) {
    var cf = new JmsConnectionFactory(env.url());
    var counter = 0;
    try (var ctx = cf.createContext(getUsername(), getPassword(), CLIENT_ACKNOWLEDGE)) {
      var queue = ctx.createQueue(queueName);
      try (var consumer = ctx.createConsumer(queue)) {
        Message message;
        for (var i = 0; i < amount; i++) {
          message = consumer.receiveNoWait();
          if (message != null) {
            message.acknowledge(); // at least once delivery
            counter++;
          }
        }
      } catch (JMSException e) {
        e.printStackTrace();
      }
    }
    LOG.info("Purged {} messages from {}:{} queue.", counter, env.name(), queueName);
  }

  /**
   * This registers a listener to pick up messages as they arrive in the queue.
   *
   * @param env Environment of the broker.
   * @param queueName The queue name.
   */
  public static void listenForMessages(Environment env, String queueName) {
    LOG.info("Listening for messages on {} from {} queue.", env.name(), queueName);
    var cf = new JmsConnectionFactory(env.url());
    try (var ctx = cf.createContext(getUsername(), getPassword())) {
      ctx.createConsumer(ctx.createQueue(queueName))
          .setMessageListener(
              DisplayUtils::createStringFromMessage);
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
            "Consumed from Queue={}:{}, Message->{}",
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
            "Moved from Queue={}:{} to Queue={}:{}, Message->{}",
            env.name(),
            fromQueueName,
            env.name(),
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
            "Moved from Queue={}:{} to Queue={}:{}, Message->{}",
            env.name(),
            fromQueueName,
            env.name(),
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
        jmsMove(ctx.createProducer(), consumer, ctx.createQueue(toQueueName));
      } catch (JMSException e) {
        e.printStackTrace();
      }
      LOG.info("Moved {} messages from {} to {} in {}.", counter, fromQueueName, toQueueName, env.name());
    }
  }

  public static void moveAllSpecificMessages(
      Environment env, String fromQueueName, String selector, String toQueueName) {
    var cf = new JmsConnectionFactory(env.url());
    try (var ctx = cf.createContext(getUsername(), getPassword(), CLIENT_ACKNOWLEDGE)) {
      var fromQ = ctx.createQueue(fromQueueName);
      var counter = 0;
      try (var consumer = ctx.createConsumer(fromQ, selector)) {
        jmsMove(ctx.createProducer(), consumer, ctx.createQueue(toQueueName));
      } catch (JMSException e) {
        e.printStackTrace();
      }
      LOG.info("Moved {} messages from {} to {} in {}, for selector={}.", counter, toQueueName, fromQueueName, env.name(),
          selector);
    }
  }

  /**
   * A utility method that moves messages from one JMS queue to another.
   * @param producer JMSProducer used for the target queue.
   * @param consumer JMSConsumer configured as the source queue.
   * @param toQueue The target queue.
   */
  private static void jmsMove(JMSProducer producer, JMSConsumer consumer, Queue toQueue)
      throws JMSException {
    var moreMessages = true;
    var counter = 0;
    Message message;
    while (moreMessages) {
      message = consumer.receive(2_000L);
      if (message != null) {
        counter++;
        producer.send(toQueue, message);
        message.acknowledge();
        LOG.info(
            "Moved to Queue={}, counter={}",
            toQueue.getQueueName(),
            counter);
      } else {
        moreMessages = false;
      }
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
      LOG.info("Moved {} messages from {}:{} to {}:{}, for selector={}.", counter, env.name(),
          fromQueueName, env.name(),
          toQueueName, selector);
    }
  }

}

