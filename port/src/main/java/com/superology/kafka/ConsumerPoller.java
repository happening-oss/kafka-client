package com.superology.kafka;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;
import com.ericsson.otp.erlang.*;

/*
 * Runs the kafka poller loop in a separate thread, and dispatches the polled
 * records to Elixir (via @ConsumerNotifier).
 *
 * In addition, this class accepts acknowledgments (acks) from Elixir, which are
 * passed via {@link ConsumerPort}. Acks are used for backpressure and commits.
 * See {@link ConsumerBackpressure} and {@link ConsumerCommits} for details.
 */
final class ConsumerPoller
    implements Runnable, ConsumerRebalanceListener {
  private Properties consumerProps;
  private Collection<TopicPartition> subscriptions;
  private ConsumerNotifier notifier;
  private Properties pollerProps;
  private BlockingQueue<Object> commands = new LinkedBlockingQueue<>();
  private ConsumerCommits commits;
  private ConsumerBackpressure backpressure;
  private HashSet<ConsumerPosition> endOffsets;

  public static ConsumerPoller start(
      Properties consumerProps,
      Collection<TopicPartition> subscriptions,
      Properties pollerProps,
      ConsumerNotifier notifier) {
    var poller = new ConsumerPoller(consumerProps, subscriptions, pollerProps, notifier);

    // Using a daemon thread to ensure program termination if the main thread stops.
    var consumerThread = new Thread(poller);
    consumerThread.setDaemon(true);
    consumerThread.start();

    return poller;
  }

  private ConsumerPoller(
      Properties consumerProps,
      Collection<TopicPartition> subscriptions,
      Properties pollerProps,
      ConsumerNotifier notifier) {
    this.consumerProps = consumerProps;
    this.subscriptions = subscriptions;
    this.notifier = notifier;
    this.pollerProps = pollerProps;
  }

  @Override
  public void run() {
    try (var consumer = new Consumer(consumerProps)) {
      startConsuming(consumer);

      var pollInterval = (int) pollerProps.getOrDefault("poll_interval", 10);
      var commitInterval = (int) pollerProps.getOrDefault("commmit_interval", 5000);
      commits = new ConsumerCommits(consumer, commitInterval);
      backpressure = new ConsumerBackpressure(consumer);

      while (true) {
        // commands issued by Elixir, such as ack or stop
        for (var command : commands())
          handleCommand(consumer, command);

        // Backpressure and commits are collected while handling Elixir
        // commands. Now we're flushing the final state (pauses and commits).
        backpressure.flush();
        if (!isAnonymous())
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

  private void handleCommand(Consumer consumer, Object command) throws Exception {
    if (command instanceof ConsumerPosition) {
      var ack = (ConsumerPosition) command;
      backpressure.recordProcessed(ack.partition());
      if (isAnonymous()) {
        if (endOffsets != null) {
          endOffsets.remove(new ConsumerPosition(ack.partition(), ack.offset() + 1));
          maybeEmitCaughtUp();
        }
      } else
        commits.add(ack.partition(), ack.offset());
    } else if (command.equals("stop")) {
      if (!isAnonymous())
        commits.flush(true);

      consumer.close();
      System.exit(0);
    } else if (command.equals("committed_offsets"))
      notifyElixir(committedOffsetsToOtp(consumer.committed(consumer.assignment())));
    else
      throw new Exception("unknown command " + command);
  }

  public void addCommand(Object command) {
    commands.add(command);
  }

  private void startConsuming(Consumer consumer) throws InterruptedException {
    if (isAnonymous()) {
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
      notifier.emit(new OtpErlangAtom("caught_up"));
      endOffsets = null;
    }
  }

  private boolean isAnonymous() {
    return consumerProps.getProperty("group.id") == null;
  }

  private List<Object> commands() {
    var result = new ArrayList<Object>();
    commands.drainTo(result);
    return result;
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
      notifier.emit(payload);
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
}

final class Consumer extends KafkaConsumer<String, byte[]> {
  public Consumer(Properties properties) {
    super(properties);
  }
}

record ConsumerPosition(TopicPartition partition, long offset) {
}
