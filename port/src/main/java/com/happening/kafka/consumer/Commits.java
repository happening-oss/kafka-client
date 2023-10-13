package com.happening.kafka.consumer;

import java.io.Serial;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/*
 * Responsible for committing offsets to Kafka. This class aggregates pending
 * commits and periodically flushes them to Kafka.
 */
final class Commits {
    private final PartitionOffsets pendingCommits = new PartitionOffsets();
    private final Consumer consumer;
    private final long commitIntervalNs;
    private long lastCommit;

    Commits(Consumer consumer, long commitIntervalMs) {
        this.consumer = consumer;
        this.commitIntervalNs = Duration.ofMillis(commitIntervalMs).toNanos();
        this.lastCommit = System.nanoTime() - this.commitIntervalNs;
    }

    void add(TopicPartition partition, long offset) {
        this.pendingCommits.put(partition, new OffsetAndMetadata(offset + 1));
    }

    void flush(boolean sync) {
        var now = System.nanoTime();
        if (now - this.lastCommit >= this.commitIntervalNs) {
            this.pendingCommits.keySet().retainAll(this.consumer.assignment());
            if (!this.pendingCommits.isEmpty()) {
                if (sync) {
                    this.consumer.commitSync(this.pendingCommits);
                } else {
                    this.consumer.commitAsync(this.pendingCommits, null);
                }

                this.pendingCommits.clear();
                this.lastCommit = now;
            }
        }
    }

    void partitionsRevoked(Collection<TopicPartition> partitions) {
        // We're losing some partitions, but there's still time to commit the offsets.
        var commits = new PartitionOffsets();
        for (var partition : partitions) {
            var offset = this.pendingCommits.remove(partition);
            if (offset != null) {
                commits.put(partition, offset);
            }
        }

        // Sync committing, because we want to block the callback until we commit.
        try {
            this.consumer.commitSync(commits, Duration.ofSeconds(5));
        } catch (Exception e) {
            // An exception here is not tragic, it just means we failed to commit during a
            // rebalance. Therefore we'll just swallow and keep going.
        }
    }

    void partitionsLost(Collection<TopicPartition> partitions) {
        // We can't commit here since the partitions have already been lost.
        this.pendingCommits.keySet().removeAll(partitions);
    }

    private static final class PartitionOffsets extends HashMap<TopicPartition, OffsetAndMetadata> {
        @Serial
        private static final long serialVersionUID = 6778668172812382107L;
    }
}
