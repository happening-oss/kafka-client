package com.superology.kafka;

import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;

final class Commits {
  PartitionOffsets pendingCommits = new PartitionOffsets();
  Consumer consumer;
  long commitIntervalNs;
  long lastCommit;

  public Commits(Consumer consumer, long commitIntervalMs) {
    this.consumer = consumer;
    this.commitIntervalNs = java.time.Duration.ofMillis(commitIntervalMs).toNanos();
    this.lastCommit = System.nanoTime() - commitIntervalNs;
  }

  public void add(TopicPartition partition, long offset) {
    pendingCommits.put(partition, new OffsetAndMetadata(offset + 1));
  }

  public void flush(boolean sync) {
    var now = System.nanoTime();
    if (now - lastCommit >= commitIntervalNs) {
      pendingCommits.keySet().retainAll(consumer.assignment());
      if (!pendingCommits.isEmpty()) {
        if (sync)
          consumer.commitSync(pendingCommits);
        else
          consumer.commitAsync(pendingCommits, null);

        pendingCommits.clear();
        lastCommit = now;
      }
    }
  }

  public void partitionsRevoked(Collection<TopicPartition> partitions) {
    PartitionOffsets commits = new PartitionOffsets();
    for (var partition : partitions) {
      var offset = pendingCommits.remove(partition);
      if (offset != null)
        commits.put(partition, offset);
    }

    try {
      consumer.commitSync(commits, Duration.ofMillis(500));
    } catch (Exception e) {
    }
  }

  public void partitionsLost(Collection<TopicPartition> partitions) {
    pendingCommits.keySet().removeAll(partitions);
  }

  final class PartitionOffsets extends HashMap<TopicPartition, OffsetAndMetadata> {
    public PartitionOffsets() {
      super();
    }
  }
}
