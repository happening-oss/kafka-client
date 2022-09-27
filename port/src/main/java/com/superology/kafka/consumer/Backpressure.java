package com.superology.kafka.consumer;

import java.util.*;

import org.apache.kafka.common.*;
import org.apache.kafka.clients.consumer.*;

/**
 * This class implements the backpressure flow of the poller. The poller
 * immediately sends all polled records to Elixir. However, using this class,
 * the poller keeps track of the records sent to Elixir but not yet processed.
 * These records are kept as a collection of queues, one per each partition.
 * When Elixir processes a record, it will send back an ack message to the
 * poller, which will in turn remove the record from the corresponding queue.
 *
 * When a record is added to the queue, if the queue becomes large (in length or
 * in total byte size), polling from the corresponding partition is paused.
 *
 * When a record is removed from the queue, if the queue is small enough,
 * polling from the corresponding partition will be resumed.
 */
final class Backpressure {
  private Consumer consumer;
  private HashSet<TopicPartition> pausedPartitions = new HashSet<TopicPartition>();
  private HashMap<TopicPartition, Queue> queues = new HashMap<TopicPartition, Queue>();

  public Backpressure(Consumer consumer) {
    this.consumer = consumer;
  }

  // Invoked by the poller when a record is polled from the broker.
  public void recordPolled(ConsumerRecord<String, byte[]> record) {
    var partition = new TopicPartition(record.topic(), record.partition());
    var queue = queues.get(partition);
    if (queue == null) {
      queue = new Queue();
      queues.put(partition, queue);
    }

    queue.recordPolled(record);
    if (queue.shouldPause())
      pausedPartitions.add(partition);
  }

  // Invoked by the poller when a record has been fully processed in Elixir.
  public void recordProcessed(TopicPartition partition) {
    var queue = queues.get(partition);
    if (queue != null) {
      queue.recordProcessed();

      if (queue.shouldResume())
        pausedPartitions.remove(partition);
    }
  }

  // Invoked to flush all pauses/resumes.
  public void flush() {
    queues.keySet().retainAll(consumer.assignment());

    // Note that we're not clearing pauses. That way we'll end up invoking `pause`
    // before every poll, but that's ok, because it's an idempotent in-memory
    // operation. By doing this we ensure that a partition remains paused after a
    // rebalance.
    pausedPartitions.retainAll(consumer.assignment());
    consumer.pause(pausedPartitions);

    // Resumed partitions are all assigned partitions which are not paused. Just
    // like with pauses, this will end up resuming non-paused partitions, but that's
    // a non-op, so it's fine.
    var resumedPartitions = new HashSet<TopicPartition>();
    resumedPartitions.addAll(consumer.assignment());
    resumedPartitions.removeAll(pausedPartitions);
    consumer.resume(resumedPartitions);
  }

  // Invoked by the poller when the partitions are lost due to a rebalance.
  public void removePartitions(Collection<TopicPartition> partitions) {
    pausedPartitions.removeAll(partitions);
  }

  final class Queue {
    private LinkedList<Integer> messageSizes = new LinkedList<>();
    private int totalBytes = 0;

    public boolean isEmpty() {
      return numMessages() == 0;
    }

    public void recordPolled(ConsumerRecord<String, byte[]> record) {
      var messageSize = record.value().length;
      messageSizes.add(messageSize);
      totalBytes += messageSize;
    }

    public void recordProcessed() {
      var messageSize = messageSizes.remove();
      totalBytes -= messageSize;
    }

    public boolean shouldPause() {
      return (numMessages() >= 2 && (totalBytes >= 1000000 || numMessages() >= 1000));
    }

    public boolean shouldResume() {
      return (numMessages() < 2 || (totalBytes <= 500000 && numMessages() <= 500));
    }

    private int numMessages() {
      return messageSizes.size();
    }
  }
}
