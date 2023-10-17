package com.happening.kafka.consumer;

import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * This class implements the backpressure flow of the poller. The poller immediately sends all polled records to Elixir.
 * However, using this class, the poller keeps track of the records sent to Elixir but not yet processed. These records
 * are kept as a collection of queues, one per each partition. When Elixir processes a record, it will send back an ack
 * message to the poller, which will in turn remove the record from the corresponding queue.
 *
 * When a record is added to the queue, if the queue becomes large (in length or in total byte size), polling from the
 * corresponding partition is paused.
 *
 * When a record is removed from the queue, if the queue is small enough, polling from the corresponding partition will
 * be resumed.
 */
final class Backpressure {
    private final Consumer consumer;
    private final Set<TopicPartition> pausedPartitions = new HashSet<>();
    private final Map<TopicPartition, Queue> queues = new HashMap<>();

    Backpressure(Consumer consumer) {
        this.consumer = consumer;
    }

    // Invoked by the poller when a record is polled from the broker.
    void recordPolled(ConsumerRecord<byte[], byte[]> record) {
        var partition = new TopicPartition(record.topic(), record.partition());
        var queue = this.queues.get(partition);
        if (queue == null) {
            queue = new Queue();
            this.queues.put(partition, queue);
        }

        queue.recordPolled(record);
        if (queue.shouldPause()) {
            this.pausedPartitions.add(partition);
        }
    }

    // Invoked by the poller when a record has been fully processed in Elixir.
    void recordProcessed(TopicPartition partition) {
        var queue = this.queues.get(partition);
        if (queue != null) {
            queue.recordProcessed();

            if (queue.shouldResume()) {
                this.pausedPartitions.remove(partition);
            }
        }
    }

    // Invoked to flush all pauses/resumes.
    void flush() {
        this.queues.keySet().retainAll(this.consumer.assignment());

        // Note that we're not clearing pauses. That way we'll end up invoking `pause`
        // before every poll, but that's ok, because it's an idempotent in-memory
        // operation. By doing this we ensure that a partition remains paused after a
        // rebalance.
        this.pausedPartitions.retainAll(this.consumer.assignment());
        this.consumer.pause(this.pausedPartitions);

        // Resumed partitions are all assigned partitions which are not paused. Just
        // like with pauses, this will end up resuming non-paused partitions, but that's
        // a non-op, so it's fine.
        var resumedPartitions = new HashSet<>(this.consumer.assignment());
        resumedPartitions.removeAll(this.pausedPartitions);
        this.consumer.resume(resumedPartitions);
    }

    // Invoked by the poller when the partitions are lost due to a rebalance.
    void removePartitions(Collection<TopicPartition> partitions) {
        this.pausedPartitions.removeAll(partitions);
    }

    static final class Queue {

        private static final int PAUSE_THRESHOLD_BYTES = 1000000;
        private static final int PAUSE_THRESHOLD_MESSAGES = 1000;
        private static final int RESUME_THRESHOLD_BYTES = 500000;
        private static final int RESUME_THRESHOLD_MESSAGES = 500;

        private final Deque<Integer> messageSizes = new LinkedList<>();
        private int totalBytes = 0;

        void recordPolled(ConsumerRecord<byte[], byte[]> record) {
            var messageSize = 0;
            if (record.value() != null) {
                messageSize = record.value().length;
            }
            this.messageSizes.add(messageSize);
            this.totalBytes += messageSize;
        }

        void recordProcessed() {
            var messageSize = this.messageSizes.remove();
            this.totalBytes -= messageSize;
        }

        boolean shouldPause() {
            return this.numMessages() >= 2 && (this.totalBytes >= PAUSE_THRESHOLD_BYTES || this.numMessages() >= PAUSE_THRESHOLD_MESSAGES);
        }

        boolean shouldResume() {
            return this.numMessages() < 2 || this.totalBytes <= RESUME_THRESHOLD_BYTES && this.numMessages() <= RESUME_THRESHOLD_MESSAGES;
        }

        private int numMessages() {
            return this.messageSizes.size();
        }
    }
}
