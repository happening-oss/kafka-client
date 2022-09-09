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
    // We're losing some partitions, but there's still time to commit the offsets.
    PartitionOffsets commits = new PartitionOffsets();
    for (var partition : partitions) {
      var offset = pendingCommits.remove(partition);
      if (offset != null)
        commits.put(partition, offset);
    }

    // Sync committing, because we want to block the callback until we commit.
    try {
      consumer.commitSync(commits, Duration.ofSeconds(5));
    } catch (Exception e) {
      // An exception here is not tragic, it just means we failed to commit during a
      // rebalance. Therefore we'll just swallow and keep going.
    }
  }

  public void partitionsLost(Collection<TopicPartition> partitions) {
    // We can't commit here since the partitions have already been lost.
    pendingCommits.keySet().removeAll(partitions);
  }

  final class PartitionOffsets extends HashMap<TopicPartition, OffsetAndMetadata> {
    public PartitionOffsets() {
      super();
    }
  }
}
