package com.superology.kafka;

import java.util.*;
import java.util.concurrent.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;
import com.ericsson.otp.erlang.*;

final class ConsumerPoller
    implements Runnable, ConsumerRebalanceListener {
  private Properties consumerProps;
  private Collection<String> topics;
  private ConsumerOutput output;
  private Properties pollerProps;
  private BlockingQueue<OtpErlangTuple> messages = new LinkedBlockingQueue<>();

  public static ConsumerPoller start(
      Properties consumerProps,
      Collection<String> topics,
      Properties pollerProps,
      ConsumerOutput output) {
    var poller = new ConsumerPoller(consumerProps, topics, pollerProps, output);

    var consumerThread = new Thread(poller);
    consumerThread.setDaemon(true);
    consumerThread.start();

    return poller;
  }

  private ConsumerPoller(
      Properties consumerProps,
      Collection<String> topics,
      Properties pollerProps,
      ConsumerOutput output) {
    this.consumerProps = consumerProps;
    this.topics = topics;
    this.output = output;
    this.pollerProps = pollerProps;
  }

  @Override
  public void run() {
    try (var consumer = new KafkaConsumer<String, byte[]>(consumerProps)) {
      startConsuming(consumer);

      var pollInterval = (int) pollerProps.getOrDefault("poll_interval", 10);
      var commitInterval = (int) pollerProps.getOrDefault("commmit_interval", 5000);
      var commits = new Commits(consumer, commitInterval);
      var backpressure = new Backpressure(consumer);

      while (true) {
        for (var message : messages()) {
          if (message.elementAt(0).toString().equals("ack")) {
            var topic = new String(((OtpErlangBinary) message.elementAt(1)).binaryValue());
            var partitionNo = ((OtpErlangLong) message.elementAt(2)).intValue();
            var partition = new TopicPartition(topic, partitionNo);
            var offset = ((OtpErlangLong) message.elementAt(3)).longValue();

            if (!isAnonymous())
              commits.add(partition, offset);

            backpressure.recordProcessed(partition);
          }
        }

        if (!isAnonymous())
          commits.flush();

        backpressure.flush();

        var records = consumer.poll(java.time.Duration.ofMillis(pollInterval));

        for (var record : records) {
          writeToOutput(record);

          backpressure.recordProcessing(
              new TopicPartition(record.topic(), record.partition()),
              record.value().length);
        }
      }
    } catch (Exception e) {
      System.err.println(e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

  public void push(OtpErlangTuple message) {
    messages.add(message);
  }

  private void startConsuming(KafkaConsumer<String, byte[]> consumer) throws InterruptedException {
    if (isAnonymous()) {
      var allPartitions = new ArrayList<TopicPartition>();
      for (var topic : topics) {
        for (var partitionInfo : consumer.partitionsFor(topic)) {
          allPartitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
        }
      }
      consumer.assign(allPartitions);

      var highWatermarks = consumer.endOffsets(allPartitions).entrySet().stream()
          .map(entry -> new OtpErlangTuple(new OtpErlangObject[] {
              new OtpErlangBinary(entry.getKey().topic().getBytes()),
              new OtpErlangInt(entry.getKey().partition()),
              new OtpErlangLong(entry.getValue())
          }))
          .toArray(OtpErlangTuple[]::new);

      output.write(new OtpErlangTuple(new OtpErlangObject[] {
          new OtpErlangAtom("end_offsets"),
          new OtpErlangList(highWatermarks) }));

    } else
      consumer.subscribe(topics, this);
  }

  private boolean isAnonymous() {
    return consumerProps.getProperty("group.id") == null;
  }

  private ArrayList<OtpErlangTuple> messages() {
    var drainedMessages = new ArrayList<OtpErlangTuple>();
    messages.drainTo(drainedMessages);
    return drainedMessages;
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
  }

  @Override
  public void onPartitionsLost(Collection<TopicPartition> partitions) {
    writeToOutput(new OtpErlangTuple(new OtpErlangObject[] {
        new OtpErlangAtom("partitions_lost"),
        toErlangList(partitions) }));
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    writeToOutput(new OtpErlangTuple(new OtpErlangObject[] {
        new OtpErlangAtom("partitions_assigned"),
        toErlangList(partitions) }));
  }

  private void writeToOutput(Object message) {
    try {
      output.write(message);
    } catch (InterruptedException e) {
      throw new org.apache.kafka.common.errors.InterruptException(e);
    }
  }

  static private OtpErlangList toErlangList(Collection<TopicPartition> partitions) {
    return new OtpErlangList(
        partitions.stream()
            .map(partition -> new OtpErlangTuple(new OtpErlangObject[] {
                new OtpErlangBinary(partition.topic().getBytes()),
                new OtpErlangInt(partition.partition())
            }))
            .toArray(OtpErlangTuple[]::new));
  }
}
