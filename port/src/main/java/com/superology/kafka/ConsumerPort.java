package com.superology.kafka;

import java.util.*;
import org.apache.kafka.common.*;

/*
 * Implements the main thread of the consumer port. Messages are received as
 * encoded Erlang/Elixir terms (`term_to_binary`).
 *
 * These messages are processed by the poller loop (see {@link ConsumerPoller}).
 * Replying to Elixir is done in the notification thread (see {@link
 * ConsumerNotifier}).
 *
 */
public class ConsumerPort implements Port {
  public static void main(String[] args) {
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");
    PortDriver.run(args, new ConsumerPort());
  }

  private ConsumerPoller poller;

  @Override
  public void initialize(Object[] args) {
    poller = startPoller(args);
  }

  @Override
  public void handleCommand(String name, Object[] args) {
    try {
      switch (name) {
        case "ack":
          poller.addCommand(toConsumerPosition(args));
          break;

        case "stop":
          poller.addCommand("stop");
          break;

        case "committed_offsets":
          poller.addCommand("committed_offsets");
          break;

        default:
          throw new Exception("unknown command " + name);
      }
    } catch (Exception e) {
      // need to rewrap into an unchecked exception because this is an interface
      // implementation
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private static ConsumerPoller startPoller(Object[] args) {
    var consumerPropsMap = (Map<Object, Object>) args[0];
    // need to remove nulls, because Properties doesn't support them
    consumerPropsMap.values().removeAll(Collections.singleton(null));
    var consumerProps = new Properties();
    consumerProps.putAll(consumerPropsMap);

    var subscriptions = new ArrayList<TopicPartition>();
    for (var subscriptionObj : (Iterable<Object>) args[1]) {
      var subscription = (Object[]) subscriptionObj;
      var topic = (String) subscription[0];
      var partition = (int) subscription[1];
      subscriptions.add(new TopicPartition(topic, partition));
    }
    var pollerProps = (Map<String, Object>) args[2];

    return ConsumerPoller.start(consumerProps, subscriptions, pollerProps, ConsumerNotifier.start());
  }

  private static ConsumerPosition toConsumerPosition(Object[] ackArgs) {
    var topic = (String) ackArgs[0];
    var partitionNo = (int) ackArgs[1];
    var partition = new TopicPartition(topic, partitionNo);

    long offset;
    if (ackArgs[2] instanceof Long)
      offset = (long) ackArgs[2];
    else
      offset = (int) ackArgs[2];

    return new ConsumerPosition(partition, offset);
  }
}
