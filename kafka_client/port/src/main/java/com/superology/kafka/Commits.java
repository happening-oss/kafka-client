package com.superology.kafka;

import java.util.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;

final class Commits {
  HashMap<TopicPartition, OffsetAndMetadata> pendingCommits = new HashMap<TopicPartition, OffsetAndMetadata>();
  KafkaConsumer<String, byte[]> consumer;
  long commitIntervalNs;
  long lastCommit;

  public Commits(KafkaConsumer<String, byte[]> consumer, long commitIntervalMs) {
    this.consumer = consumer;
    this.commitIntervalNs = java.time.Duration.ofMillis(commitIntervalMs).toNanos();
    this.lastCommit = System.nanoTime() - commitIntervalNs;
  }

  public void add(TopicPartition partition, long offset) {
    pendingCommits.put(partition, new OffsetAndMetadata(offset + 1));
  }

  public void flush() {
    var now = System.nanoTime();
    if (now - lastCommit >= commitIntervalNs) {
      pendingCommits.keySet().retainAll(consumer.assignment());
      if (!pendingCommits.isEmpty()) {
        consumer.commitAsync(pendingCommits, null);
        pendingCommits.clear();
        lastCommit = now;
      }
    }
  }

  public void partitionsRevoked(Collection<TopicPartition> partitions) {
    HashMap<TopicPartition, OffsetAndMetadata> commits = new HashMap<TopicPartition, OffsetAndMetadata>();
    for (var partition : partitions) {
      var offset = pendingCommits.remove(partition);
      if (offset != null)
        commits.put(partition, offset);
    }

    consumer.commitAsync(commits, null);
  }

  public void partitionsLost(Collection<TopicPartition> partitions) {
    pendingCommits.keySet().removeAll(partitions);
  }
}
