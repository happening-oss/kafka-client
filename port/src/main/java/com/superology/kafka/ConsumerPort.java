package com.superology.kafka;

import java.util.*;
import java.util.stream.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;
import com.ericsson.otp.erlang.*;

/*
 * Implements the consumer port, which runs the kafka poller loop, and
 * dispatches the polled records to Elixir.
 *
 * In addition, the consumer accepts acknowledgments (acks) from Elixir, which
 * are used for backpressure and commits. See {@link ConsumerBackpressure} and
 * {@link ConsumerCommits} for details.
 *
 * See {@link PortDriver} for information about port mechanics, such as
 * communication protocol and thread.
 */
public class ConsumerPort implements Port, ConsumerRebalanceListener {
  public static void main(String[] args) {
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");
    PortDriver.run(args, new ConsumerPort());
  }

  private PortOutput output;
  private ConsumerCommits commits;
  private ConsumerBackpressure backpressure;
  private HashSet<ConsumerPosition> endOffsets;
  private boolean isAnonymous;

  @Override
  @SuppressWarnings("unchecked")
  public void run(PortWorker worker, PortOutput output, Object[] args) {
    this.output = output;
    var consumerProps = mapToProperties((Map<Object, Object>) args[0]);

    isAnonymous = (consumerProps.getProperty("group.id") == null);

    var subscriptions = new ArrayList<TopicPartition>();
    for (var subscription : (Iterable<Object[]>) args[1])
      subscriptions.add(new TopicPartition((String) subscription[0], (int) subscription[1]));

    try (var consumer = new Consumer(consumerProps)) {
      startConsuming(consumer, subscriptions);

      Map<String, Object> pollerProps = (Map<String, Object>) args[2];
      var pollInterval = (int) pollerProps.getOrDefault("poll_interval", 10);
      var commitInterval = (int) pollerProps.getOrDefault("commmit_interval", 5000);
      commits = new ConsumerCommits(consumer, commitInterval);
      backpressure = new ConsumerBackpressure(consumer);

      while (true) {
        // commands issued by Elixir, such as ack or stop
        for (var command : worker.drainCommands())
          handleCommand(consumer, command);

        // Backpressure and commits are collected while handling Elixir
        // commands. Now we're flushing the final state (pauses and commits).
        backpressure.flush();
        if (!isAnonymous)
          commits.flush(false);

        var records = consumer.poll(java.time.Duration.ofMillis(pollInterval));

        for (var record : records) {
          // Each record is sent separately to Elixir, instead of sending them
          // all at once. This improves the throughput, since Elixir can start
          // processing each record as soon as it arrives, instead of waiting
          // for all the records to be received.
          notifyElixir(recordToOtp(record));
          backpressure.recordPolled(record);
        }
      }
    } catch (Exception e) {
      System.err.println(e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

  private void handleCommand(Consumer consumer, Port.Command command) throws Exception {
    switch (command.name()) {
      case "ack":
        var topic = (String) command.args()[0];
        var partitionNo = (int) command.args()[1];
        var partition = new TopicPartition(topic, partitionNo);

        long offset;
        if (command.args()[2] instanceof Long)
          offset = (long) command.args()[2];
        else
          offset = (int) command.args()[2];

        var ack = new ConsumerPosition(partition, offset);

        backpressure.recordProcessed(ack.partition());
        if (isAnonymous) {
          if (endOffsets != null) {
            endOffsets.remove(new ConsumerPosition(ack.partition(), ack.offset() + 1));
            maybeEmitCaughtUp();
          }
        } else
          commits.add(ack.partition(), ack.offset());
        break;

      case "stop":
        if (!isAnonymous)
          commits.flush(true);

        consumer.close();
        System.exit(0);
        break;

      case "committed_offsets":
        notifyElixir(committedOffsetsToOtp(consumer.committed(consumer.assignment())));
        break;

      default:
        throw new Exception("unknown command " + command.name());
    }
  }

  private void startConsuming(Consumer consumer, Collection<TopicPartition> subscriptions) throws InterruptedException {
    if (isAnonymous) {
      // When not in a consumer group we need to manually self-assign the desired
      // partitions
      var assignments = new ArrayList<TopicPartition>();
      for (var subscription : subscriptions) {
        if (subscription.partition() >= 0)
          // client is interested in a particular topic-partition
          assignments.add(subscription);
        else
          // client wants to consume the entire topic
          for (var partitionInfo : consumer.partitionsFor(subscription.topic()))
            assignments.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
      }
      consumer.assign(assignments);

      // We'll also fire the assigned notification manually, since the
      // onPartitionsAssigned callback is not invoked on manual assignment. This
      // keeps the behaviour consistent and supports synchronism on the
      // processor side.
      onPartitionsAssigned(assignments);

      // We'll also store the end offsets of all assigned partitions. This
      // allows us to fire the "caught_up" notification, issued after all the
      // records, existing at the time of the assignment, are processed.
      this.endOffsets = new HashSet<>();
      for (var entry : consumer.endOffsets(assignments).entrySet()) {
        if (entry.getValue() > 0)
          this.endOffsets.add(new ConsumerPosition(entry.getKey(), entry.getValue()));
      }

      maybeEmitCaughtUp();
    } else {
      // in a consumer group -> subscribe to the desired topics
      var topics = StreamSupport.stream(subscriptions.spliterator(), false)
          .map(subscription -> subscription.topic()).distinct()
          .toList();
      consumer.subscribe(topics, this);
    }
  }

  private void maybeEmitCaughtUp() throws InterruptedException {
    if (endOffsets.isEmpty()) {
      output.emit(new OtpErlangAtom("caught_up"));
      endOffsets = null;
    }
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    emitRebalanceEvent("assigned", partitions);
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    commits.partitionsRevoked(partitions);
    backpressure.removePartitions(partitions);
    emitRebalanceEvent("unassigned", partitions);
  }

  @Override
  public void onPartitionsLost(Collection<TopicPartition> partitions) {
    commits.partitionsLost(partitions);
    backpressure.removePartitions(partitions);
    emitRebalanceEvent("unassigned", partitions);
  }

  private void emitRebalanceEvent(String event, Collection<TopicPartition> partitions) {
    notifyElixir(new OtpErlangTuple(new OtpErlangObject[] {
        new OtpErlangAtom(event),
        new OtpErlangList(
            partitions.stream()
                .map(partition -> new OtpErlangTuple(new OtpErlangObject[] {
                    new OtpErlangBinary(partition.topic().getBytes()),
                    new OtpErlangInt(partition.partition())
                }))
                .toArray(OtpErlangTuple[]::new))
    }));
  }

  private void notifyElixir(OtpErlangObject payload) {
    try {
      output.emit(payload);
    } catch (InterruptedException e) {
      throw new org.apache.kafka.common.errors.InterruptException(e);
    }
  }

  static private OtpErlangObject recordToOtp(ConsumerRecord<String, byte[]> record) {
    var headers = StreamSupport.stream(record.headers().spliterator(), false)
        .map(header -> new OtpErlangTuple(new OtpErlangObject[] {
            new OtpErlangBinary(header.key().getBytes()),
            new OtpErlangBinary(header.value())
        })).toArray(OtpErlangTuple[]::new);

    return new OtpErlangTuple(new OtpErlangObject[] {
        new OtpErlangAtom("record"),
        new OtpErlangBinary(record.topic().getBytes()),
        new OtpErlangInt(record.partition()),
        new OtpErlangLong(record.offset()),
        new OtpErlangLong(record.timestamp()),
        new OtpErlangList(headers),
        new OtpErlangBinary(record.key().getBytes()),
        new OtpErlangBinary(record.value()),
    });
  }

  static private OtpErlangObject committedOffsetsToOtp(Map<TopicPartition, OffsetAndMetadata> map) {
    var elements = map.entrySet().stream()
        .filter(entry -> entry.getValue() != null)
        .map(entry -> new OtpErlangTuple(new OtpErlangObject[] {
            new OtpErlangBinary(entry.getKey().topic().getBytes()),
            new OtpErlangInt(entry.getKey().partition()),
            new OtpErlangLong(entry.getValue().offset())
        })).toArray(OtpErlangTuple[]::new);

    return new OtpErlangTuple(new OtpErlangObject[] {
        new OtpErlangAtom("committed"),
        new OtpErlangList(elements) });
  }

  private Properties mapToProperties(Map<Object, Object> map) {
    // need to remove nulls, because Properties doesn't support them
    map.values().removeAll(Collections.singleton(null));
    var result = new Properties();
    result.putAll(map);
    return result;
  }
}

final class Consumer extends KafkaConsumer<String, byte[]> {
  public Consumer(Properties properties) {
    super(properties);
  }
}

record ConsumerPosition(TopicPartition partition, long offset) {
}
