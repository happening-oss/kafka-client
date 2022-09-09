package com.superology.kafka;

import java.util.*;

import org.apache.kafka.common.*;
import org.apache.kafka.clients.consumer.*;

final class Backpressure {
  private Consumer consumer;
  private HashSet<TopicPartition> pausedPartitions = new HashSet<TopicPartition>();
  private HashSet<TopicPartition> resumedPartitions = new HashSet<TopicPartition>();
  private HashMap<TopicPartition, Queue> queues = new HashMap<TopicPartition, Queue>();

  public Backpressure(Consumer consumer) {
    this.consumer = consumer;
  }

  public void recordProcessing(ConsumerRecord<String, byte[]> record) {
    var partition = new TopicPartition(record.topic(), record.partition());
    var queue = queues.get(partition);
    if (queue == null) {
      queue = new Queue();
      queues.put(partition, queue);
    }

    queue.recordProcessing(record);
    if (queue.shouldPause()) {
      pausedPartitions.add(partition);
      resumedPartitions.remove(partition);
    }
  }

  public void recordProcessed(TopicPartition partition) {
    var queue = queues.get(partition);
    if (queue != null) {
      queue.recordProcessed();

      if (queue.shouldResume()) {
        pausedPartitions.remove(partition);
        resumedPartitions.add(partition);
      }
    }
  }

  public void flush() {
    queues.keySet().retainAll(consumer.assignment());

    pausedPartitions.retainAll(consumer.assignment());
    consumer.pause(pausedPartitions);
    pausedPartitions.clear();

    resumedPartitions.retainAll(consumer.assignment());
    consumer.resume(resumedPartitions);
    resumedPartitions.clear();
  }

  public void partitionsLost(Collection<TopicPartition> partitions) {
    pausedPartitions.removeAll(partitions);
    resumedPartitions.removeAll(partitions);
  }

  final class Queue {
    private LinkedList<Integer> messageSizes = new LinkedList<>();
    private int totalBytes = 0;

    public boolean isEmpty() {
      return numMessages() == 0;
    }

    public void recordProcessing(ConsumerRecord<String, byte[]> record) {
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
