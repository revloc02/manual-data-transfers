package forest.colver.datatransfer.it;

import static forest.colver.datatransfer.azure.AzureUtils.EMX_SANDBOX_EVENTGRID_HOST;
import static forest.colver.datatransfer.azure.AzureUtils.EMX_SANDBOX_EVENTGRID_SUBSCRIPTION_QUEUE;
import static forest.colver.datatransfer.azure.AzureUtils.EMX_SANDBOX_EVENTGRID_TOPIC_KEY;
import static forest.colver.datatransfer.azure.AzureUtils.EMX_SANDBOX_NAMESPACE;
import static forest.colver.datatransfer.azure.AzureUtils.EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY;
import static forest.colver.datatransfer.azure.AzureUtils.EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY;
import static forest.colver.datatransfer.azure.AzureUtils.buildAsbConnectionString;
import static forest.colver.datatransfer.azure.AzureUtils.createEvent;
import static forest.colver.datatransfer.azure.EventGridOperations.aegSendMessage;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbConsume;
import static forest.colver.datatransfer.azure.ServiceBusQueueOperations.asbQueuePurge;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests for Azure Event Grid.
 *
 * <p><b>How Azure Event Grid works:</b>
 *
 * <ol>
 *   <li><b>Topic</b> — the entry point. You publish events to a topic's HTTPS endpoint (the
 *       "host"), authenticated with a topic key.
 *   <li><b>Event</b> — a JSON object with required fields: {@code id}, {@code subject}, {@code
 *       data}, {@code eventType}, {@code eventTime}, {@code dataVersion}. See {@link
 *       forest.colver.datatransfer.azure.AzureUtils#createEvent}.
 *   <li><b>Subscriptions</b> — Event Grid routes events from a topic to one or more subscribers
 *       (Service Bus queues, Storage queues, webhooks, Functions, etc.). Subscriptions are
 *       configured in Azure (here: terraform in {@code devops/terraform/azure/main.tf}), not in
 *       code. Filtering rules are also configured there.
 *   <li><b>Delivery</b> — Event Grid wraps your event payload and delivers it to each subscriber.
 *       For an ASB queue subscription, the event arrives as a JSON message in the queue body.
 * </ol>
 *
 * <p>The setup for these tests: an Event Grid topic with a Service Bus queue subscription. We
 * publish to the topic, then read from the queue to verify delivery.
 *
 * <p><b>Why each test purges the queue first:</b> the subscribed ASB queue is shared across runs.
 * If a previous run's events are still sitting on it, a fresh consume might pull a stale message
 * instead of ours. {@link #purgeBeforeEach} drains the queue so each test starts clean.
 *
 * <p><b>Why we poll for "our" message by id:</b> publishing to Event Grid is async — the event
 * takes a moment to reach the subscription queue. A consume immediately after publish often returns
 * nothing or returns an unrelated message that happened to land first. Each test generates a unique
 * {@code id} and polls until that exact message appears.
 */
class AzureEventGridTests {

  private static final Logger LOG = LoggerFactory.getLogger(AzureEventGridTests.class);
  private static final String EVENTGRID_HOST = EMX_SANDBOX_EVENTGRID_HOST;
  private static final String EVENTGRID_TOPIC_KEY = EMX_SANDBOX_EVENTGRID_TOPIC_KEY;
  private static final String CONN_STR =
      buildAsbConnectionString(
          EMX_SANDBOX_NAMESPACE,
          EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_POLICY,
          EMX_SANDBOX_NAMESPACE_SHARED_ACCESS_KEY);
  private static final String SUBSCRIPTION_QUEUE = EMX_SANDBOX_EVENTGRID_SUBSCRIPTION_QUEUE;

  @BeforeEach
  void purgeBeforeEach() {
    var purged = asbQueuePurge(CONN_STR, SUBSCRIPTION_QUEUE);
    LOG.info("Purged {} stale messages from subscription queue before test.", purged);
  }

  /**
   * Consume messages from the subscription queue until we find the one whose payload {@code id}
   * matches the expected value, or until the timeout expires. Returns the matched event payload as
   * a Map.
   *
   * <p>Why this loop is needed: see the class-level Javadoc — Event Grid delivery is async, and
   * other publishers may share the topic. Filtering by id ensures we assert against our own event,
   * not someone else's.
   */
  private HashMap<String, Object> awaitEventWithId(String expectedId) {
    var mapper = new ObjectMapper();
    var holder = new HashMap<String, HashMap<String, Object>>();
    await()
        .pollInterval(Duration.ofSeconds(2))
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(
            () -> {
              var maybe = asbConsume(CONN_STR, SUBSCRIPTION_QUEUE);
              assertThat(maybe).isPresent();
              var msg = maybe.get();
              @SuppressWarnings("unchecked")
              var event =
                  (HashMap<String, Object>)
                      mapper.readValue(msg.getBody().toBytes(), HashMap.class);
              assertThat(event.get("id")).isEqualTo(expectedId);
              holder.put("event", event);
            });
    return holder.get("event");
  }

  /**
   * End-to-end: publish an event to the topic, then consume from the subscribed ASB queue and
   * verify the payload survived the round trip.
   *
   * <p>Requires the sandbox resources from {@code devops/terraform/azure/main.tf} to be
   * provisioned. If this test fails on connection, that's where to start looking.
   */
  @Test
  void testSend() throws IOException {
    // Unique id per run so we can recognize our own message on the shared queue.
    var id = "testSend-" + UUID.randomUUID();
    var subject = "AppEventA";
    var body = "this is the body";
    var eventType = "test.event.type";
    var dataVersion = "0.0.1";

    // Publish to Event Grid. The 5-arg createEvent fills eventTime with "now" in UTC, which is
    // what real events should claim. Use the 6-arg overload only when a test needs a fixed
    // timestamp to assert on.
    aegSendMessage(
        EVENTGRID_HOST,
        EVENTGRID_TOPIC_KEY,
        createEvent(id, subject, body, eventType, dataVersion));

    // Poll until our event arrives. Event Grid delivery is async and the queue may have other
    // traffic — we filter by id to make sure we're asserting on our own event.
    var event = awaitEventWithId(id);

    // Verify the round trip preserved the payload. Note: we don't assert on eventTime since
    // it's "now" — but you could capture it before sending and assert it lands within a window.
    assertEquals(body, event.get("data"));
    assertEquals(eventType, event.get("eventType"));
    assertEquals(subject, event.get("subject"));
  }

  /**
   * Same round-trip as testSend, but uses the 6-arg {@link
   * forest.colver.datatransfer.azure.AzureUtils#createEvent} overload to inject a known timestamp.
   * Demonstrates when you'd reach for the 6-arg version: the test asserts that eventTime survives
   * the round trip exactly, which only works with a fixed value.
   *
   * <p>Real-world reasons you might want a controlled timestamp: replaying historical events,
   * testing time-windowed subscription filters, or asserting that a downstream consumer reads
   * eventTime correctly.
   */
  @Test
  void testSendWithFixedEventTime() throws IOException {
    var id = "testSendFixed-" + UUID.randomUUID();
    var subject = "AppEventA";
    var body = "this is the body";
    var eventType = "test.event.type";
    var dataVersion = "0.0.1";
    // A known, arbitrary timestamp. The format is ISO-8601 with explicit UTC offset, which is
    // what Event Grid's schema requires. Anything else gets rejected at publish time.
    var fixedEventTime = "2026-01-15T10:30:00+00:00";

    aegSendMessage(
        EVENTGRID_HOST,
        EVENTGRID_TOPIC_KEY,
        createEvent(id, subject, body, eventType, fixedEventTime, dataVersion));

    var event = awaitEventWithId(id);

    // The payoff: with a fixed timestamp we can assert the exact value round-tripped. Event
    // Grid may normalize the format (e.g. "+00:00" → "Z"), so we compare as parsed instants
    // rather than raw strings to be tolerant of representation differences.
    assertEquals(body, event.get("data"));
    assertEquals(eventType, event.get("eventType"));
    assertEquals(subject, event.get("subject"));
    var receivedEventTime = OffsetDateTime.parse((String) event.get("eventTime")).toInstant();
    var sentEventTime = OffsetDateTime.parse(fixedEventTime).toInstant();
    assertEquals(sentEventTime, receivedEventTime);
  }
}
