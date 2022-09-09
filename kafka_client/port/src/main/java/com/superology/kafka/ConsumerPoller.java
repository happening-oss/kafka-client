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
  private ConsumerNotifier notifier;
  private Properties pollerProps;
  private BlockingQueue<Object> commands = new LinkedBlockingQueue<>();
  private Commits commits;
  private ConsumerBackpressure backpressure;

  public static ConsumerPoller notifier(
      Properties consumerProps,
      Collection<String> topics,
      Properties pollerProps,
      ConsumerNotifier notifier) {
    var poller = new ConsumerPoller(consumerProps, topics, pollerProps, notifier);

    // Using a daemon thread to ensure program termination if the main thread stops.
    var consumerThread = new Thread(poller);
    consumerThread.setDaemon(true);
    consumerThread.start();

    return poller;
  }

  private ConsumerPoller(
      Properties consumerProps,
      Collection<String> topics,
      Properties pollerProps,
      ConsumerNotifier notifier) {
    this.consumerProps = consumerProps;
    this.topics = topics;
    this.notifier = notifier;
    this.pollerProps = pollerProps;
  }

  @Override
  public void run() {
    try (var consumer = new Consumer(consumerProps)) {
      startConsuming(consumer);

      var pollInterval = (int) pollerProps.getOrDefault("poll_interval", 10);
      var commitInterval = (int) pollerProps.getOrDefault("commmit_interval", 5000);
      commits = new Commits(consumer, commitInterval);
      backpressure = new ConsumerBackpressure(consumer);

      while (true) {
        // commands issued by Elixir, such as ack or stop
        for (var command : commands())
          handleCommand(consumer, command);

        // flushing after all commands are handled
        backpressure.flush();
        if (!isAnonymous())
          commits.flush(false);

        var records = consumer.poll(java.time.Duration.ofMillis(pollInterval));

        for (var record : records) {
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
    if (command instanceof Ack) {
      var ack = (Ack) command;
      backpressure.recordProcessed(ack.partition());
      if (!isAnonymous())
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
      var allPartitions = new ArrayList<TopicPartition>();
      for (var topic : topics) {
        for (var partitionInfo : consumer.partitionsFor(topic)) {
          allPartitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
        }
      }
      consumer.assign(allPartitions);
      notifier.emit(endOffsetsToOtp(consumer.endOffsets(allPartitions)));

    } else
      consumer.subscribe(topics, this);
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
    notifyElixir(new OtpErlangTuple(new OtpErlangObject[] { new OtpErlangAtom(event), toErlangList(partitions) }));
  }

  private void notifyElixir(OtpErlangObject payload) {
    try {
      notifier.emit(payload);
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

  static private OtpErlangObject recordToOtp(ConsumerRecord<String, byte[]> record) {
    return new OtpErlangTuple(new OtpErlangObject[] {
        new OtpErlangAtom("record"),
        new OtpErlangBinary(record.topic().getBytes()),
        new OtpErlangInt(record.partition()),
        new OtpErlangLong(record.offset()),
        new OtpErlangLong(record.timestamp()),
        new OtpErlangBinary(record.value())
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

  static private OtpErlangObject endOffsetsToOtp(Map<TopicPartition, Long> map) {
    var elements = map.entrySet().stream()
        .map(entry -> new OtpErlangTuple(new OtpErlangObject[] {
            new OtpErlangBinary(entry.getKey().topic().getBytes()),
            new OtpErlangInt(entry.getKey().partition()),
            new OtpErlangLong(entry.getValue())
        }))
        .toArray(OtpErlangTuple[]::new);

    return new OtpErlangTuple(new OtpErlangObject[] {
        new OtpErlangAtom("end_offsets"),
        new OtpErlangList(elements) });
  }
}

final class Consumer extends KafkaConsumer<String, byte[]> {
  public Consumer(Properties properties) {
    super(properties);
  }
}

record Ack(TopicPartition partition, long offset) {
}

record CommittedRequest(TopicPartition partition) {
}
