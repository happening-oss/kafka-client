package com.happening.kafka.consumer;

import com.ericsson.otp.erlang.OtpErlangAtom;
import com.ericsson.otp.erlang.OtpErlangBinary;
import com.ericsson.otp.erlang.OtpErlangInt;
import com.ericsson.otp.erlang.OtpErlangLong;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.happening.kafka.port.Driver;
import com.happening.kafka.port.Erlang;
import com.happening.kafka.port.Output;
import com.happening.kafka.port.Port;
import com.happening.kafka.port.Worker;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;

/*
 * Implements the consumer port, which runs the kafka poller loop, and
 * dispatches the polled records to Elixir.
 *
 * In addition, the consumer accepts acknowledgments (acks) from Elixir, which
 * are used for backpressure and commits. See {@link Backpressure} and {@link
 * Commits} for details.
 *
 * See {@link com.happening.kafka.port.Driver} for information about port
 * mechanics, such as communication protocol and thread.
 */
public class Main implements Port, ConsumerRebalanceListener {
    public static void main(String[] args) {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");
        Driver.run(args, new Main());
    }

    private static final int POLL_DURATION_MS = 10;
    private static final int COMMIT_INTERVAL_MS = 5000;

    private Output output;
    private Commits commits;
    private Backpressure backpressure;
    private Map<TopicPartition, Long> endOffsets;
    private boolean isAnonymous;

    private final Map<String, Handler> dispatchMap = Map.ofEntries(
            Map.entry("stop", this::stop),
            Map.entry("ack", this::ack)
    );

    @Override
    public int run(Worker worker, Output output, Object[] args) throws Exception {
        this.output = output;

        var opts = this.opts(args);
        this.isAnonymous = (opts.consumerProps().getProperty("group.id") == null);

        try (var consumer = new Consumer(opts.consumerProps())) {
            if (this.isAnonymous) {
                this.startAnonymousConsuming(consumer, opts.subscriptions);
            } else {
                this.startConsumerGroupConsuming(consumer, opts.subscriptions);
            }

            var pollDuration = (int) opts.pollerProps().getOrDefault("poll_duration", POLL_DURATION_MS);
            var commitInterval = (int) opts.pollerProps().getOrDefault("commit_interval", COMMIT_INTERVAL_MS);
            this.commits = new Commits(consumer, commitInterval);
            this.backpressure = new Backpressure(consumer);

            while (true) {
                // commands issued by Elixir, such as ack or stop
                for (var command : worker.drainCommands()) {
                    var exitCode = this.dispatchMap.get(command.name()).handle(consumer, command);
                    if (exitCode != null) {
                        return exitCode;
                    }
                }

                // Backpressure and commits are collected while handling Elixir
                // commands. Now we're flushing the final state (pauses and commits).
                this.backpressure.flush();
                if (!this.isAnonymous) {
                    this.commits.flush(false);
                }

                var records = consumer.poll(Duration.ofMillis(pollDuration));

                for (var record : records) {
                    // Each record is sent separately to Elixir, instead of sending them
                    // all at once. This improves the throughput, since Elixir can start
                    // processing each record as soon as it arrives, instead of waiting
                    // for all the records to be received.
                    output.emit(recordToOtp(record), true);
                    this.backpressure.recordPolled(record);
                }
            }
        }
    }

    private Opts opts(Object[] args) {
        @SuppressWarnings("unchecked")
        var consumerProps = this.mapToProperties((Map<Object, Object>) args[0]);

        var subscriptions = new ArrayList<Subscription>();

        for (@SuppressWarnings("unchecked")
        var subscription : (Iterable<Object[]>) args[1]) {
            var topic = (String) subscription[0];
            var partitionNo = (int) subscription[1];
            var partition = new TopicPartition(topic, partitionNo);
            Integer type = null;
            Long position = null;

            if (subscription[2] != null) {
                type = (int) subscription[2];
                position = toLong(subscription[3]);
            }
            subscriptions.add(new Subscription(partition, type, position));
        }

        @SuppressWarnings("unchecked")
        var pollerProps = (Map<String, Object>) args[2];

        return new Opts(consumerProps, subscriptions, pollerProps);
    }

    private int stop(Consumer consumer, Port.Command command) {
        if (!this.isAnonymous) {
            this.commits.flush(true);
        }

        return 0;
    }

    private Integer ack(Consumer consumer, Port.Command command) throws InterruptedException {
        for (@SuppressWarnings("unchecked")
        var record : (Iterable<List<Object>>) command.args()[0]) {
            var array = record.toArray();
            var ack = new ConsumerPosition(toTopicPartition(array), toLong(array[2]));

            this.backpressure.recordProcessed(ack.partition());

            if (this.isAnonymous) {
                if (this.endOffsets != null) {

                    var endOffset = this.endOffsets.get(ack.partition);
                    if (endOffset != null && endOffset - 1 <= ack.offset()) {
                        this.endOffsets.remove(ack.partition());
                    }

                    this.maybeEmitCaughtUp();
                }
            } else {
                this.commits.add(ack.partition(), ack.offset());
            }
        }

        return null;
    }

    private static TopicPartition toTopicPartition(Object[] args) {
        var topic = (String) args[0];
        var partitionNo = (int) args[1];
        return new TopicPartition(topic, partitionNo);
    }

    private static long toLong(Object value) {
        if (value instanceof Long) {
            return (long) value;
        }

        return (int) value;
    }

    private void startConsumerGroupConsuming(Consumer consumer, Collection<Subscription> subscriptions) {
        // in a consumer group -> subscribe to the desired topics
        var topics = subscriptions.stream().map(
                subscription -> subscription.partition().topic()
        ).distinct().toList();
        consumer.subscribe(topics, this);
    }

    private void startAnonymousConsuming(
            Consumer consumer, Collection<Subscription> subscriptions
    ) throws InterruptedException {
        // When not in a consumer group we need to manually self-assign the desired
        // partitions
        var assignments = this.assignPartitions(consumer, subscriptions);

        // In this mode the consumer may also ask to start at a particular position
        this.seekToPositions(consumer, subscriptions);

        // We'll also fire the assigned notification manually, since the
        // onPartitionsAssigned callback is not invoked on manual assignment. This
        // keeps the behaviour consistent and supports synchronism on the
        // processor side.
        this.onPartitionsAssigned(assignments);

        // We'll also store the end offsets of all assigned partitions. This
        // allows us to fire the "caught_up" notification, issued after all the
        // records, existing at the time of the assignment, are processed.
        this.endOffsets = new HashMap<>();

        // The caught up event should not be emitted as long as there is a
        // difference between the current consumer position and the end offset
        // for a specific partition.
        for (var entry : consumer.endOffsets(assignments).entrySet()) {
            if (entry.getValue() != consumer.position(entry.getKey())) {
                this.endOffsets.put(entry.getKey(), entry.getValue());
            }
        }

        this.maybeEmitCaughtUp();
    }

    private ArrayList<TopicPartition> assignPartitions(Consumer consumer, Collection<Subscription> subscriptions) {
        var assignments = new ArrayList<TopicPartition>();
        for (var subscription : subscriptions) {
            if (subscription.partition().partition() >= 0)
            // client is interested in a particular topic-partition
            {
                assignments.add(subscription.partition());
            } else
            // client wants to consume the entire topic
            {
                for (var partitionInfo : consumer.partitionsFor(subscription.partition().topic())) {
                    assignments.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
                }
            }
        }
        consumer.assign(assignments);
        return assignments;
    }

    private void seekToPositions(Consumer consumer, Collection<Subscription> subscriptions) {
        var subscriptionsWithType = subscriptions.stream().filter(subscription -> subscription.type() != null).toList();
        this.seekToOffsets(consumer, subscriptionsWithType.stream().filter(subscription -> subscription.type() == 0));
        this.seekToTimestamps(
                consumer, subscriptionsWithType.stream().filter(subscription -> subscription.type() == 1)
        );
    }

    private void seekToOffsets(Consumer consumer, Stream<Subscription> subscriptions) {
        subscriptions.forEach(subscription -> {
            if (subscription.partition().partition() >= 0) {
                consumer.seek(subscription.partition(), subscription.position());
            } else {
                this.seekAllPartitionsToPosition(consumer, subscription);
            }
        });
    }

    private void seekAllPartitionsToPosition(Consumer consumer, Subscription subscription) {
        consumer.partitionsFor(subscription.partition().topic()).forEach(
                partitionInfo -> consumer.seek(this.toTopicPartition(partitionInfo), subscription.position())
        );
    }

    private void seekToTimestamps(Consumer consumer, Stream<Subscription> subscriptions) {
        var timestampsToSearch = new HashMap<TopicPartition, Long>();

        subscriptions.forEach(subscription -> {
            if (subscription.partition().partition() >= 0) {
                timestampsToSearch.put(subscription.partition(), subscription.position());
            } else {
                this.addTimestampsForAllPartitions(consumer, timestampsToSearch, subscription);
            }
        });

        consumer.offsetsForTimes(timestampsToSearch).forEach((topicPartition, offsetAndTimestamp) -> {
            if (offsetAndTimestamp != null) {
                consumer.seek(topicPartition, offsetAndTimestamp.offset());
            }
        });
    }

    private void addTimestampsForAllPartitions(
            Consumer consumer,
            Map<TopicPartition, Long> timestampsToSearch,
            Subscription subscription
    ) {

        consumer.partitionsFor(subscription.partition().topic())
                .forEach(partitionInfo ->
                        timestampsToSearch.put(this.toTopicPartition(partitionInfo), subscription.position())
                );
    }

    private void maybeEmitCaughtUp() throws InterruptedException {
        if (this.endOffsets.isEmpty()) {
            this.output.emit(new OtpErlangAtom("caught_up"));
            this.endOffsets = null;
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        this.emitRebalanceEvent("assigned", partitions);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        this.commits.partitionsRevoked(partitions);
        this.backpressure.removePartitions(partitions);
        this.emitRebalanceEvent("unassigned", partitions);
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        this.commits.partitionsLost(partitions);
        this.backpressure.removePartitions(partitions);
        this.emitRebalanceEvent("unassigned", partitions);
    }

    private void emitRebalanceEvent(String event, Collection<TopicPartition> partitions) {
        try {
            this.output.emit(
                    Erlang.tuple(
                            Erlang.atom(event),
                            Erlang.toList(
                                    partitions,
                                    partition -> Erlang.tuple(
                                            Erlang.binary(partition.topic()),
                                            Erlang.integer(partition.partition())
                                    )
                            )
                    )
            );
        } catch (InterruptedException e) {
            throw new InterruptException(e);
        }
    }

    private static OtpErlangObject recordToOtp(ConsumerRecord<byte[], byte[]> record) {
        var headers = Erlang.toList(
                record.headers(),
                header -> Erlang.tuple(
                        new OtpErlangBinary(header.key().getBytes()),
                        new OtpErlangBinary(header.value())
                )
        );

        return Erlang.tuple(
                new OtpErlangAtom("record"),
                new OtpErlangBinary(record.topic().getBytes()),
                new OtpErlangInt(record.partition()),
                new OtpErlangLong(record.offset()),
                new OtpErlangLong(record.timestamp()),
                headers,
                record.key() == null ? Erlang.nil() : new OtpErlangBinary(record.key()),
                record.value() == null ? Erlang.nil() : new OtpErlangBinary(record.value())
        );
    }

    private Properties mapToProperties(Map<Object, Object> map) {
        // need to remove nulls, because Properties doesn't support them
        map.values().removeAll(Collections.singleton(null));
        var result = new Properties();
        result.putAll(map);
        return result;
    }

    private TopicPartition toTopicPartition(PartitionInfo partitionInfo) {
        return new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
    }

    private record Opts(Properties consumerProps, Collection<Subscription> subscriptions,
                        Map<String, Object> pollerProps) {
    }

    private record ConsumerPosition(TopicPartition partition, long offset) {
    }

    private record Subscription(TopicPartition partition, Integer type, Long position) {
    }

    @FunctionalInterface
    interface Handler {
        Integer handle(Consumer consumer, Port.Command command) throws Exception;
    }
}

final class Consumer extends KafkaConsumer<byte[], byte[]> {
    Consumer(Properties properties) {
        super(properties);
    }
}
