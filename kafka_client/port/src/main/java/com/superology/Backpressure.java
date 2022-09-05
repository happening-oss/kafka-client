package com.superology;

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

  public void recordProcessing(TopicPartition topicPartition, int bytes) {
    var bufferUsage = bufferUsages.get(topicPartition);
    if (bufferUsage == null) {
      bufferUsage = new BufferUsage();
      bufferUsages.put(topicPartition, bufferUsage);
    }

    bufferUsage.recordProcessing(bytes);
    if (bufferUsage.shouldPause()) {
      pausedPartitions.add(topicPartition);
      resumedPartitions.remove(topicPartition);
    }
  }

  public void recordProcessed(TopicPartition topicPartition) {
    var bufferUsage = bufferUsages.get(topicPartition);
    if (bufferUsage != null) {
      bufferUsage.recordProcessed();

      if (bufferUsage.shouldResume()) {
        pausedPartitions.remove(topicPartition);
        resumedPartitions.add(topicPartition);
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
