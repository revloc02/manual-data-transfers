package forest.colver.datatransfer.messaging;

import static forest.colver.datatransfer.config.Utils.getPassword;
import static forest.colver.datatransfer.config.Utils.getUsername;

import javax.jms.Message;
import org.apache.qpid.jms.JmsConnectionFactory;

public class JmsConsume {

  public static Message consumeOneMessage(Environment env, String fromQueueName) {
    var cf = new JmsConnectionFactory(env.url());
    try (var ctx = cf.createContext(getUsername(), getPassword())) {
      var fromQ = ctx.createQueue(fromQueueName);
      try (var consumer = ctx.createConsumer(fromQ)) {
        return consumer.receive(5_000L);
      }
    }
  }
}
