package com.superology.kafka;

import java.util.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;

final class Backpressure {
  private KafkaConsumer<String, byte[]> consumer;
  private HashSet<TopicPartition> pausedPartitions = new HashSet<TopicPartition>();
  private HashSet<TopicPartition> resumedPartitions = new HashSet<TopicPartition>();
  private HashMap<TopicPartition, BufferUsage> bufferUsages = new HashMap<TopicPartition, BufferUsage>();

  public Backpressure(KafkaConsumer<String, byte[]> consumer) {
    this.consumer = consumer;
  }

  public void recordProcessing(TopicPartition partition, int bytes) {
    var bufferUsage = bufferUsages.get(partition);
    if (bufferUsage == null) {
      bufferUsage = new BufferUsage();
      bufferUsages.put(partition, bufferUsage);
    }

    bufferUsage.recordProcessing(bytes);
    if (bufferUsage.shouldPause()) {
      pausedPartitions.add(partition);
      resumedPartitions.remove(partition);
    }
  }

  public void recordProcessed(TopicPartition partition) {
    var bufferUsage = bufferUsages.get(partition);
    if (bufferUsage != null) {
      bufferUsage.recordProcessed();

      if (bufferUsage.shouldResume()) {
        pausedPartitions.remove(partition);
        resumedPartitions.add(partition);
      }
    }
  }

  public void flush() {
    bufferUsages.keySet().retainAll(consumer.assignment());

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
}

final class BufferUsage {
  private LinkedList<Integer> messageSizes = new LinkedList<>();
  private int totalBytes = 0;

  public boolean isEmpty() {
    return numMessages() == 0;
  }

  public void recordProcessing(int messageSize) {
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
