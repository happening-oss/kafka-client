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

  private Map<String, Handler> dispatchMap = Map.ofEntries(
      Map.entry("stop", this::stop),
      Map.entry("ack", this::ack),
      Map.entry("committed_offsets", this::committedOffsets));

  @Override
  public int run(PortWorker worker, PortOutput output, Object[] args) throws Exception {
    this.output = output;

    var opts = opts(args);
    isAnonymous = (opts.consumerProps().getProperty("group.id") == null);

    try (var consumer = new Consumer(opts.consumerProps())) {
      if (isAnonymous)
        startAnonymousConsuming(consumer, opts.subscriptions);
      else
        startConsumerGroupConsuming(consumer, opts.subscriptions);

      var pollInterval = (int) opts.pollerProps().getOrDefault("poll_interval", 10);
      var commitInterval = (int) opts.pollerProps().getOrDefault("commmit_interval", 5000);
      commits = new ConsumerCommits(consumer, commitInterval);
      backpressure = new ConsumerBackpressure(consumer);

      while (true) {
        // commands issued by Elixir, such as ack or stop
        for (var command : worker.drainCommands()) {
          var exitCode = dispatchMap.get(command.name()).handle(consumer, command);
          if (exitCode != null)
            return exitCode;
        }

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
          output.emit(recordToOtp(record), true);
          backpressure.recordPolled(record);
        }
      }
    }
  }

  private Opts opts(Object[] args) {
    @SuppressWarnings("unchecked")
    var consumerProps = mapToProperties((Map<Object, Object>) args[0]);

    var subscriptions = new ArrayList<Subscription>();

    for (@SuppressWarnings("unchecked")
    var subscription : (Iterable<Object[]>) args[1]) {
      var topic = (String) subscription[0];
      var partitionNo = (int) subscription[1];
      var partition = new TopicPartition(topic, partitionNo);
      Integer type = null;
      Long position = null;

      if (subscription[2] != null) {
        type = (int) subscription[2];
        position = toLong(subscription[3]);
      }
      subscriptions.add(new Subscription(partition, type, position));
    }

    @SuppressWarnings("unchecked")
    var pollerProps = (Map<String, Object>) args[2];

    return new Opts(consumerProps, subscriptions, pollerProps);
  }

  private int stop(Consumer consumer, Port.Command command) {
    if (!isAnonymous)
      commits.flush(true);

    return 0;
  }

  private Integer ack(Consumer consumer, Port.Command command) throws InterruptedException {
    var ack = new ConsumerPosition(toTopicPartition(command.args()), toLong(command.args()[2]));

    backpressure.recordProcessed(ack.partition());
    if (isAnonymous) {
      if (endOffsets != null) {
        endOffsets.remove(new ConsumerPosition(ack.partition(), ack.offset() + 1));
        maybeEmitCaughtUp();
      }
    } else
      commits.add(ack.partition(), ack.offset());

    return null;
  }

  private Integer committedOffsets(Consumer consumer, Port.Command command) throws InterruptedException {
    output.emitCallResponse(
        command,
        committedOffsetsToOtp(consumer.committed(consumer.assignment())));

    return null;
  }

  private static TopicPartition toTopicPartition(Object[] args) {
    var topic = (String) args[0];
    var partitionNo = (int) args[1];
    return new TopicPartition(topic, partitionNo);
  }

  private static long toLong(Object value) {
    if (value instanceof Long)
      return (long) value;

    return (long) ((int) value);
  }

  private void startConsumerGroupConsuming(Consumer consumer, Collection<Subscription> subscriptions) {
    // in a consumer group -> subscribe to the desired topics
    var topics = StreamSupport.stream(subscriptions.spliterator(), false)
        .map(subscription -> subscription.partition().topic()).distinct()
        .toList();
    consumer.subscribe(topics, this);
  }

  private void startAnonymousConsuming(Consumer consumer, Collection<Subscription> subscriptions)
      throws InterruptedException {
    // When not in a consumer group we need to manually self-assign the desired
    // partitions
    var assignments = assignPartitions(consumer, subscriptions);

    // In this mode the consumer may also ask to start at a particular position
    seekToPositions(consumer, subscriptions);

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
  }

  private ArrayList<TopicPartition> assignPartitions(Consumer consumer, Collection<Subscription> subscriptions) {
    var assignments = new ArrayList<TopicPartition>();
    for (var subscription : subscriptions) {
      if (subscription.partition().partition() >= 0)
        // client is interested in a particular topic-partition
        assignments.add(subscription.partition());
      else
        // client wants to consume the entire topic
        for (var partitionInfo : consumer.partitionsFor(subscription.partition().topic()))
          assignments.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
    }
    consumer.assign(assignments);
    return assignments;
  }

  private void seekToPositions(Consumer consumer, Collection<Subscription> subscriptions) {
    var timestampsToSearch = new HashMap<TopicPartition, Long>();

    for (var subscription : subscriptions) {
      if (subscription.type() != null) {
        switch (subscription.type()) {
          case 0:
            consumer.seek(subscription.partition(), subscription.position());
            break;

          case 1:
            timestampsToSearch.put(subscription.partition(), subscription.position());
            break;
        }
      }
    }

    for (var positionEntry : consumer.offsetsForTimes(timestampsToSearch).entrySet()) {
      if (positionEntry.getValue() != null)
        consumer.seek(positionEntry.getKey(), positionEntry.getValue().offset());
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
    try {
      output.emit(new OtpErlangTuple(new OtpErlangObject[] {
          new OtpErlangAtom(event),
          Erlang.toList(
              partitions,
              partition -> new OtpErlangTuple(new OtpErlangObject[] {
                  new OtpErlangBinary(partition.topic().getBytes()),
                  new OtpErlangInt(partition.partition())
              }))
      }));
    } catch (InterruptedException e) {
      throw new org.apache.kafka.common.errors.InterruptException(e);
    }
  }

  static private OtpErlangObject recordToOtp(ConsumerRecord<String, byte[]> record) {
    var headers = Erlang.toList(
        record.headers(),
        header -> new OtpErlangTuple(new OtpErlangObject[] {
            new OtpErlangBinary(header.key().getBytes()),
            new OtpErlangBinary(header.value())
        }));

    return new OtpErlangTuple(new OtpErlangObject[] {
        new OtpErlangAtom("record"),
        new OtpErlangBinary(record.topic().getBytes()),
        new OtpErlangInt(record.partition()),
        new OtpErlangLong(record.offset()),
        new OtpErlangLong(record.timestamp()),
        headers,
        new OtpErlangBinary(record.key().getBytes()),
        new OtpErlangBinary(record.value()),
    });
  }

  static private OtpErlangObject committedOffsetsToOtp(Map<TopicPartition, OffsetAndMetadata> map) {
    return Erlang.toList(
        map.entrySet().stream().filter(entry -> entry.getValue() != null).toList(),
        entry -> new OtpErlangTuple(new OtpErlangObject[] {
            new OtpErlangBinary(entry.getKey().topic().getBytes()),
            new OtpErlangInt(entry.getKey().partition()),
            new OtpErlangLong(entry.getValue().offset())
        }));
  }

  private Properties mapToProperties(Map<Object, Object> map) {
    // need to remove nulls, because Properties doesn't support them
    map.values().removeAll(Collections.singleton(null));
    var result = new Properties();
    result.putAll(map);
    return result;
  }

  record Opts(Properties consumerProps, Collection<Subscription> subscriptions, Map<String, Object> pollerProps) {
  }

  record ConsumerPosition(TopicPartition partition, long offset) {
  }

  record Subscription(TopicPartition partition, Integer type, Long position) {
  }

  @FunctionalInterface
  interface Handler {
    Integer handle(Consumer consumer, Port.Command command) throws Exception;
  }
}

final class Consumer extends KafkaConsumer<String, byte[]> {
  public Consumer(Properties properties) {
    super(properties);
  }
}
