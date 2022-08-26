package com.superology;

import java.util.Collection;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import com.ericsson.otp.erlang.*;

final class KafkaConsumerPoller implements Runnable {
  private Properties consumerProps;
  private Collection<String> topics;
  private KafkaConsumerOutput output;
  private long pollInterval;

  public static KafkaConsumerPoller start(
      Properties consumerProps,
      Collection<String> topics,
      long pollInterval,
      KafkaConsumerOutput output) {
    var poller = new KafkaConsumerPoller(consumerProps, topics, pollInterval, output);

    var consumerThread = new Thread(poller);
    consumerThread.setDaemon(true);
    consumerThread.start();

    return poller;
  }

  private KafkaConsumerPoller(
      Properties consumerProps,
      Collection<String> topics,
      long pollInterval,
      KafkaConsumerOutput output) {
    this.consumerProps = consumerProps;
    this.topics = topics;
    this.output = output;
    this.pollInterval = pollInterval;
  }

  @Override
  public void run() {
    try (var consumer = new KafkaConsumer<String, byte[]>(consumerProps)) {
      System.out.println("subscribing to: " + topics.toString() + "\r");
      consumer.subscribe(topics);

      while (true) {
        var isConsuming = !consumer.assignment().isEmpty();
        var records = consumer.poll(java.time.Duration.ofMillis(pollInterval));

        if (!isConsuming && !consumer.assignment().isEmpty()) {
          var message = new OtpErlangAtom("consuming");
          output.write(message);
          isConsuming = true;
        }

        if (!records.isEmpty())
          output.write(records);
      }
    } catch (Exception e) {
      System.err.println(e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }
}
