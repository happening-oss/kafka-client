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
  private BlockingQueue<Ack> acks = new LinkedBlockingQueue<>();
  private Commits commits;
  private Backpressure backpressure;

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
      commits = new Commits(consumer, commitInterval);
      backpressure = new Backpressure(consumer);

      while (true) {
        for (var ack : acks()) {
          backpressure.recordProcessed(ack.partition());
          if (!isAnonymous())
            commits.add(ack.partition(), ack.offset());
        }

        backpressure.flush();
        if (!isAnonymous())
          commits.flush();

        var records = consumer.poll(java.time.Duration.ofMillis(pollInterval));

        for (var record : records) {
          writeToOutput(toOtp(record));

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

  public void ack(Ack ack) {
    acks.add(ack);
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

  private List<Ack> acks() {
    var result = new ArrayList<Ack>();
    acks.drainTo(result);
    return result;
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    emitRebalanceEvent("assigned", partitions);
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    backpressure.partitionsLost(partitions);
    commits.partitionsRevoked(partitions);
    emitRebalanceEvent("unassigned", partitions);
  }

  @Override
  public void onPartitionsLost(Collection<TopicPartition> partitions) {
    backpressure.partitionsLost(partitions);
    commits.partitionsLost(partitions);
    emitRebalanceEvent("unassigned", partitions);
  }

  private void emitRebalanceEvent(String event, Collection<TopicPartition> partitions) {
    writeToOutput(new OtpErlangTuple(new OtpErlangObject[] { new OtpErlangAtom(event), toErlangList(partitions) }));
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

  static private OtpErlangTuple toOtp(ConsumerRecord<String, byte[]> record) {
    return new OtpErlangTuple(new OtpErlangObject[] {
        new OtpErlangAtom("record"),
        new OtpErlangBinary(record.topic().getBytes()),
        new OtpErlangInt(record.partition()),
        new OtpErlangLong(record.offset()),
        new OtpErlangLong(record.timestamp()),
        new OtpErlangBinary(record.value())
    });
  }
}

record Ack(TopicPartition partition, long offset) {
}
