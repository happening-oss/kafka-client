package com.superology.kafka.admin;

import com.ericsson.otp.erlang.OtpErlangAtom;
import com.ericsson.otp.erlang.OtpErlangBinary;
import com.ericsson.otp.erlang.OtpErlangInt;
import com.ericsson.otp.erlang.OtpErlangLong;
import com.ericsson.otp.erlang.OtpErlangMap;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.superology.kafka.port.Driver;
import com.superology.kafka.port.Erlang;
import com.superology.kafka.port.Output;
import com.superology.kafka.port.Port;
import com.superology.kafka.port.Worker;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListConsumerGroupsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;

public class Main implements Port {
    public static void main(String[] args) {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");
        Driver.run(args, new Main());
    }

    private Map<String, Handler> dispatchMap = Map.ofEntries(
            Map.entry("stop", this::stop),
            Map.entry("describe_topics", this::describeTopics),
            Map.entry("describe_topics_config", this::describeTopicsConfig),
            Map.entry("list_topics", this::listTopics),
            Map.entry("list_end_offsets", this::listEndOffsets),
            Map.entry("list_earliest_offsets", this::listEarliestOffsets),
            Map.entry("list_consumer_groups", this::listConsumerGroups),
            Map.entry("describe_consumer_groups", this::describeConsumerGroups),
            Map.entry("delete_consumer_groups", this::deleteConsumerGroups),
            Map.entry("list_consumer_group_offsets", this::listConsumerGroupOffsets),
            Map.entry("create_topics", this::createTopics),
            Map.entry("delete_topics", this::deleteTopics)
    );

    @Override
    public int run(Worker worker, Output output, Object[] args) throws Exception {
        @SuppressWarnings("unchecked")
        var props = mapToProperties((Map<Object, Object>) args[0]);

        try (var admin = Admin.create(props)) {
            while (true) {
                var command = worker.take();
                var exitCode = dispatchMap.get(command.name()).handle(admin, command, output);
                if (exitCode != null) {
                    return exitCode;
                }
            }
        }
    }

    private Integer stop(Admin admin, Port.Command command, Output output) {
        return 0;
    }

    private Integer listTopics(
            Admin admin, Port.Command command, Output output
    ) throws InterruptedException, ExecutionException {
        output.emitCallResponse(
                command,
                Erlang.toList(
                        admin.listTopics().names().get(),
                        name -> new OtpErlangBinary(name.getBytes())
                )
        );

        return null;
    }

    private Integer describeTopics(Admin admin, Port.Command command, Output output) throws InterruptedException {
        @SuppressWarnings("unchecked")
        var topics = (Collection<String>) command.args()[0];
        OtpErlangObject response;
        try {
            var descriptions = admin.describeTopics(TopicCollection.ofTopicNames(topics));
            var map = Erlang.toMap(
                    descriptions.allTopicNames().get(),
                    entry -> {
                        var topic = new OtpErlangBinary(entry.getKey().getBytes());
                        var partitions = Erlang.toList(
                                entry.getValue().partitions(),
                                partition -> new OtpErlangInt(partition.partition())
                        );
                        return Erlang.mapEntry(topic, partitions);
                    }
            );

            response = Erlang.ok(map);
        } catch (ExecutionException e) {
            response = Erlang.error(new OtpErlangBinary(e.getCause().getMessage().getBytes()));
        }

        output.emitCallResponse(command, response);

        return null;
    }

    private Integer describeTopicsConfig(Admin admin, Port.Command command, Output output) throws InterruptedException {
        @SuppressWarnings("unchecked")
        var topics = (Collection<String>) command.args()[0];
        OtpErlangObject response;
        try {

            var configs = (Collection<ConfigResource>) new ArrayList<ConfigResource>();
            topics.forEach(topic -> configs.add(new ConfigResource(ConfigResource.Type.TOPIC, topic)));

            var map = Erlang.toMap(
                    admin.describeConfigs(configs).all().get(),
                    conf -> {
                        conf.getValue().entries();
                        var topic = new OtpErlangBinary(conf.getKey().name().getBytes());
                        OtpErlangObject params = Erlang.toList(
                                conf.getValue().entries(),
                                entry -> {
                                    var config = new OtpErlangMap();
                                    config.put(new OtpErlangAtom("name"), new OtpErlangBinary(entry.name().getBytes()));
                                    config.put(
                                            new OtpErlangAtom("value"), new OtpErlangBinary(entry.value().getBytes())
                                    );
                                    config.put(new OtpErlangAtom("is_default"), new OtpErlangAtom(entry.isDefault()));

                                    return config;
                                }
                        );
                        return Erlang.mapEntry(topic, params);
                    }
            );

            response = Erlang.ok(map);
        } catch (ExecutionException e) {
            response = Erlang.error(new OtpErlangBinary(e.getCause().getMessage().getBytes()));
        }

        output.emitCallResponse(command, response);

        return null;
    }

    private Integer listEndOffsets(Admin admin, Port.Command command, Output output) throws InterruptedException {

        var topicPartitionOffsets = new HashMap<TopicPartition, OffsetSpec>();
        for (@SuppressWarnings("unchecked")
        var topicPartitionTuple : ((Iterable<Object[]>) command.args()[0])) {
            var topicPartition = new TopicPartition((String) topicPartitionTuple[0], (int) topicPartitionTuple[1]);
            topicPartitionOffsets.put(topicPartition, OffsetSpec.latest());
        }

        OtpErlangObject response;
        try {
            var map = Erlang.toMap(
                    admin.listOffsets(topicPartitionOffsets).all().get(),
                    entry -> Erlang.mapEntry(
                            Erlang.tuple(
                                    new OtpErlangBinary(entry.getKey().topic().getBytes()),
                                    new OtpErlangInt(entry.getKey().partition())
                            ),
                            new OtpErlangLong(entry.getValue().offset())
                    )
            );
            response = Erlang.ok(map);
        } catch (ExecutionException e) {
            response = Erlang.error(new OtpErlangBinary(e.getCause().getMessage().getBytes()));
        }

        output.emitCallResponse(command, response);

        return null;
    }

    private Integer listEarliestOffsets(Admin admin, Port.Command command, Output output) throws InterruptedException {

        var topicPartitionOffsets = new HashMap<TopicPartition, OffsetSpec>();
        for (@SuppressWarnings("unchecked")
        var topicPartitionTuple : ((Iterable<Object[]>) command.args()[0])) {
            var topicPartition = new TopicPartition((String) topicPartitionTuple[0], (int) topicPartitionTuple[1]);
            topicPartitionOffsets.put(topicPartition, OffsetSpec.earliest());
        }

        OtpErlangObject response;
        try {
            var map = Erlang.toMap(
                    admin.listOffsets(topicPartitionOffsets).all().get(),
                    entry -> Erlang.mapEntry(
                            Erlang.tuple(
                                    new OtpErlangBinary(entry.getKey().topic().getBytes()),
                                    new OtpErlangInt(entry.getKey().partition())
                            ),
                            new OtpErlangLong(entry.getValue().offset())
                    )
            );
            response = Erlang.ok(map);
        } catch (ExecutionException e) {
            response = Erlang.error(new OtpErlangBinary(e.getCause().getMessage().getBytes()));
        }

        output.emitCallResponse(command, response);

        return null;
    }

    private Integer listConsumerGroups(Admin admin, Port.Command command, Output output) throws InterruptedException {

        var options = new ListConsumerGroupsOptions();

        OtpErlangObject response;
        try {
            var list = Erlang.toList(
                    admin.listConsumerGroups(options).valid().get(),
                    entry -> {
                        var state = entry.state().isPresent() ? new OtpErlangAtom(
                                entry.state().get().toString().toLowerCase()
                        ) : new OtpErlangAtom("undefiend");
                        return Erlang.tuple(new OtpErlangBinary(entry.groupId().getBytes()), state);
                    }
            );

            response = Erlang.ok(list);
        } catch (ExecutionException e) {
            response = Erlang.error(new OtpErlangBinary(e.getCause().getMessage().getBytes()));
        }

        output.emitCallResponse(command, response);

        return null;
    }

    private Integer describeConsumerGroups(
            Admin admin, Port.Command command, Output output
    ) throws InterruptedException {
        @SuppressWarnings("unchecked")
        var consumerGroups = (Collection<String>) command.args()[0];

        OtpErlangObject response;
        try {
            var map = Erlang.toMap(
                    admin.describeConsumerGroups(consumerGroups).all().get(),
                    entry -> {
                        var groupName = new OtpErlangBinary(entry.getKey().getBytes());

                        var members = Erlang.toList(entry.getValue().members(), member -> {
                            var consumerId = member.consumerId();
                            var assignments = Erlang.toList(
                                    member.assignment().topicPartitions(), assignment -> Erlang.tuple(
                                            new OtpErlangBinary(assignment.topic().getBytes()),
                                            new OtpErlangInt(assignment.partition())
                                    )
                            );

                            return Erlang.tuple(new OtpErlangBinary(consumerId.getBytes()), assignments);
                        });
                        OtpErlangMap description = new OtpErlangMap();
                        description.put(new OtpErlangAtom("members"), members);
                        description.put(
                                new OtpErlangAtom("state"),
                                new OtpErlangAtom(entry.getValue().state().toString().toLowerCase())
                        );

                        return Erlang.mapEntry(groupName, description);
                    }
            );

            response = Erlang.ok(map);
        } catch (ExecutionException e) {
            response = Erlang.error(new OtpErlangBinary(e.getCause().getMessage().getBytes()));
        }

        output.emitCallResponse(command, response);

        return null;
    }

    private Integer deleteConsumerGroups(Admin admin, Port.Command command, Output output) throws InterruptedException {

        @SuppressWarnings("unchecked")
        var consumerGroups = (Collection<String>) command.args()[0];
        var options = new DeleteConsumerGroupsOptions();

        OtpErlangObject response;
        try {
            var map = Erlang.toMap(
                    admin.deleteConsumerGroups(consumerGroups, options).deletedGroups(),
                    entry -> {
                        OtpErlangObject result = Erlang.ok();
                        try {
                            entry.getValue().get();
                        } catch (Exception e) {
                            result = Erlang.error(new OtpErlangBinary(e.getCause().getMessage().getBytes()));
                        }

                        return Erlang.mapEntry(new OtpErlangBinary(entry.getKey().getBytes()), result);
                    }
            );

            response = Erlang.ok(map);

        } catch (Exception e) {
            response = Erlang.error(new OtpErlangBinary(e.getCause().getMessage().getBytes()));
        }

        output.emitCallResponse(command, response);

        return null;
    }

    private Integer listConsumerGroupOffsets(
            Admin admin, Port.Command command, Output output
    ) throws InterruptedException {

        @SuppressWarnings("unchecked")
        var inputMap = (HashMap<String, Iterable<Object[]>>) command.args()[0];
        var groupSpecs = new HashMap<String, ListConsumerGroupOffsetsSpec>();

        for (var entry : inputMap.entrySet()) {
            var groupId = entry.getKey();

            var topicPartitions = new LinkedList<TopicPartition>();
            for (var topicPartitionTuple : entry.getValue()) {
                var topicPartition = new TopicPartition((String) topicPartitionTuple[0], (int) topicPartitionTuple[1]);
                topicPartitions.add(topicPartition);
            }

            var offsetsSpec = new ListConsumerGroupOffsetsSpec();
            offsetsSpec.topicPartitions(topicPartitions);

            groupSpecs.put(groupId, offsetsSpec);
        }

        var options = new ListConsumerGroupOffsetsOptions();

        OtpErlangObject response;
        try {
            var map = Erlang.toMap(
                    admin.listConsumerGroupOffsets(groupSpecs, options).all().get(),
                    entry -> {
                        var topicPartitions = Erlang.toMap(
                                entry.getValue(),
                                innerEntry -> {
                                    var value = innerEntry.getValue();
                                    OtpErlangObject offset = value == null ? Erlang.nil() : new OtpErlangLong(
                                            value.offset()
                                    );
                                    return Erlang.mapEntry(
                                            Erlang.tuple(
                                                    new OtpErlangBinary(innerEntry.getKey().topic().getBytes()),
                                                    new OtpErlangInt(innerEntry.getKey().partition())
                                            ),
                                            offset
                                    );
                                }
                        );

                        return Erlang.mapEntry(
                                new OtpErlangBinary(entry.getKey().getBytes()),
                                topicPartitions
                        );
                    }
            );

            response = Erlang.ok(map);
        } catch (ExecutionException e) {
            response = Erlang.error(new OtpErlangBinary(e.getCause().getMessage().getBytes()));
        }

        output.emitCallResponse(command, response);

        return null;
    }

    private Integer createTopics(Admin admin, Port.Command command, Output output) throws InterruptedException {
        var newTopics = new LinkedList<NewTopic>();

        for (@SuppressWarnings("unchecked")
        var newTopicTuple : ((Iterable<Object[]>) command.args()[0])) {
            newTopics.add(
                    new NewTopic(
                            (String) newTopicTuple[0],
                            Optional.ofNullable((Integer) newTopicTuple[1]),
                            Optional.empty()
                    )
            );
        }

        try {
            admin.createTopics(newTopics).all().get();
            output.emitCallResponse(command, Erlang.ok());
        } catch (ExecutionException e) {
            output.emitCallResponse(command, Erlang.error(new OtpErlangBinary(e.getCause().getMessage().getBytes())));
        }

        return null;
    }

    private Integer deleteTopics(Admin admin, Port.Command command, Output output) throws InterruptedException {
        try {
            @SuppressWarnings("unchecked")
            var topics = (Collection<String>) command.args()[0];
            admin.deleteTopics(topics).all().get();
            output.emitCallResponse(command, Erlang.ok());
        } catch (ExecutionException e) {
            output.emitCallResponse(command, Erlang.error(new OtpErlangBinary(e.getCause().getMessage().getBytes())));
        }

        return null;
    }

    private Properties mapToProperties(Map<Object, Object> map) {
        // need to remove nulls, because Properties doesn't support them
        map.values().removeAll(Collections.singleton(null));
        var result = new Properties();
        result.putAll(map);
        return result;
    }

    @FunctionalInterface
    interface Handler {
        Integer handle(Admin admin, Port.Command command, Output output) throws Exception;
    }
}
