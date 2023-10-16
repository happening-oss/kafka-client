package com.happening.kafka.admin;

import com.ericsson.otp.erlang.OtpErlangMap;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.happening.kafka.port.Driver;
import com.happening.kafka.port.Erlang;
import com.happening.kafka.port.Output;
import com.happening.kafka.port.Port;
import com.happening.kafka.port.Worker;
import com.happening.kafka.utils.ErrorUtils;
import com.happening.kafka.utils.PropertiesUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
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
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;

public class Main implements Port {
    public static void main(String[] args) {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");
        Driver.run(args, new Main());
    }

    private final Map<String, Handler> dispatchMap = Map.ofEntries(
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
        var props = PropertiesUtils.toProperties((Map<Object, Object>) args[0]);

        try (var admin = Admin.create(props)) {
            while (true) {
                var command = worker.take();
                var exitCode = this.dispatchMap.get(command.name()).handle(admin, command, output);
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
                Erlang.toList(admin.listTopics().names().get(), Erlang::binary)
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
                    stringTopicDescriptionEntry -> {
                        var topic = Erlang.binary(stringTopicDescriptionEntry.getKey());
                        var partitions = stringTopicDescriptionEntry.getValue().partitions().stream().map(
                                TopicPartitionInfo::partition
                        ).toList();
                        return Erlang.mapEntry(topic, Erlang.toList(partitions, Erlang::integer));
                    }
            );

            response = Erlang.ok(map);
        } catch (ExecutionException e) {
            response = Erlang.error(Erlang.binary(ErrorUtils.getMessage(e)));
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
                    configResourceConfigEntry -> {
                        var topic = Erlang.binary(configResourceConfigEntry.getKey().name());
                        OtpErlangObject params = Erlang.toList(
                                configResourceConfigEntry.getValue().entries(),
                                configEntry -> Erlang.mapFromEntries(
                                        Erlang.mapEntry(Erlang.atom("name"), Erlang.binary(configEntry.name())),
                                        Erlang.mapEntry(Erlang.atom("value"), Erlang.binary(configEntry.value())),
                                        Erlang.mapEntry(Erlang.atom("is_default"), Erlang.atom(configEntry.isDefault()))
                                )
                        );
                        return Erlang.mapEntry(topic, params);
                    }
            );

            response = Erlang.ok(map);
        } catch (ExecutionException e) {
            response = Erlang.error(Erlang.binary(ErrorUtils.getMessage(e)));
        }

        output.emitCallResponse(command, response);

        return null;
    }

    private Integer listEndOffsets(Admin admin, Port.Command command, Output output) throws InterruptedException {
        var topicPartitionOffsets = new HashMap<TopicPartition, OffsetSpec>();

        @SuppressWarnings("unchecked")
        var topicPartitionTuples = (Iterable<Object[]>) command.args()[0];
        for (var topicPartitionTuple : topicPartitionTuples) {
            var topicPartition = new TopicPartition((String) topicPartitionTuple[0], (int) topicPartitionTuple[1]);
            topicPartitionOffsets.put(topicPartition, OffsetSpec.latest());
        }

        OtpErlangObject response;
        try {
            var map = Erlang.toMap(
                    admin.listOffsets(topicPartitionOffsets).all().get(),
                    topicPartitionOffsetsEntry -> Erlang.mapEntry(
                            Erlang.tuple(
                                    Erlang.binary(topicPartitionOffsetsEntry.getKey().topic()),
                                    Erlang.integer(topicPartitionOffsetsEntry.getKey().partition())
                            ),
                            Erlang.longValue(topicPartitionOffsetsEntry.getValue().offset())
                    )
            );
            response = Erlang.ok(map);
        } catch (ExecutionException e) {
            response = Erlang.error(Erlang.binary(ErrorUtils.getMessage(e)));
        }

        output.emitCallResponse(command, response);

        return null;
    }

    private Integer listEarliestOffsets(Admin admin, Port.Command command, Output output) throws InterruptedException {
        var topicPartitionOffsets = new HashMap<TopicPartition, OffsetSpec>();

        @SuppressWarnings("unchecked")
        var topicPartitionTuples = (Iterable<Object[]>) command.args()[0];
        for (var topicPartitionTuple : topicPartitionTuples) {
            var topicPartition = new TopicPartition((String) topicPartitionTuple[0], (int) topicPartitionTuple[1]);
            topicPartitionOffsets.put(topicPartition, OffsetSpec.earliest());
        }

        OtpErlangObject response;
        try {
            var map = Erlang.toMap(
                    admin.listOffsets(topicPartitionOffsets).all().get(),
                    topicPartitionOffsetsEntry -> Erlang.mapEntry(
                            Erlang.tuple(
                                    Erlang.binary(topicPartitionOffsetsEntry.getKey().topic()),
                                    Erlang.integer(topicPartitionOffsetsEntry.getKey().partition())
                            ),
                            Erlang.longValue(topicPartitionOffsetsEntry.getValue().offset())
                    )
            );
            response = Erlang.ok(map);
        } catch (ExecutionException e) {
            response = Erlang.error(Erlang.binary(ErrorUtils.getMessage(e)));
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
                    consumerGroupListing -> {
                        var state = consumerGroupListing.state();
                        var erlangState = state.isPresent() ? Erlang.atom(
                                state.get().toString().toLowerCase()
                        ) : Erlang.atom("undefined");
                        return Erlang.tuple(Erlang.binary(consumerGroupListing.groupId()), erlangState);
                    }
            );

            response = Erlang.ok(list);
        } catch (ExecutionException e) {
            response = Erlang.error(Erlang.binary(ErrorUtils.getMessage(e)));
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
                    groupDescriptionEntry -> {
                        var groupName = Erlang.binary(groupDescriptionEntry.getKey());

                        var members = Erlang.toList(
                                groupDescriptionEntry.getValue().members(),
                                member -> {
                                    var consumerId = member.consumerId();
                                    var assignments = Erlang.toList(
                                            member.assignment().topicPartitions(),
                                            assignment -> Erlang.tuple(
                                                    Erlang.binary(assignment.topic()),
                                                    Erlang.integer(assignment.partition())
                                            )
                                    );

                                    return Erlang.tuple(Erlang.binary(consumerId), assignments);
                                }
                        );
                        var consumerGroupState = groupDescriptionEntry.getValue().state().toString().toLowerCase();
                        OtpErlangMap description = Erlang.mapFromEntries(
                                Erlang.mapEntry(Erlang.atom("members"), members),
                                Erlang.mapEntry(Erlang.atom("state"), Erlang.atom(consumerGroupState))
                        );
                        return Erlang.mapEntry(groupName, description);
                    }
            );

            response = Erlang.ok(map);
        } catch (ExecutionException e) {
            response = Erlang.error(Erlang.binary(ErrorUtils.getMessage(e)));
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
                    groupDeletionFutureEntry -> {
                        OtpErlangObject result = Erlang.ok();
                        try {
                            groupDeletionFutureEntry.getValue().get();
                        } catch (Exception e) {
                            result = Erlang.error(Erlang.binary(ErrorUtils.getMessage(e)));
                        }

                        return Erlang.mapEntry(Erlang.binary(groupDeletionFutureEntry.getKey()), result);
                    }
            );

            response = Erlang.ok(map);

        } catch (Exception e) {
            response = Erlang.error(Erlang.binary(ErrorUtils.getMessage(e)));
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
                    topicPartitionOffsetsEntry -> {
                        var topicPartitions = Erlang.toMap(
                                topicPartitionOffsetsEntry.getValue(),
                                partitionOffsetsEntry -> {
                                    var offsets = partitionOffsetsEntry.getValue();
                                    var partitionTopic = partitionOffsetsEntry.getKey();
                                    var offset = offsets == null ? Erlang.nil() : Erlang.longValue(offsets.offset());
                                    return Erlang.mapEntry(
                                            Erlang.tuple(
                                                    Erlang.binary(partitionTopic.topic()),
                                                    Erlang.integer(partitionTopic.partition())
                                            ),
                                            offset
                                    );
                                }
                        );

                        return Erlang.mapEntry(Erlang.binary(topicPartitionOffsetsEntry.getKey()), topicPartitions);
                    }
            );

            response = Erlang.ok(map);
        } catch (ExecutionException e) {
            response = Erlang.error(Erlang.binary(ErrorUtils.getMessage(e)));
        }

        output.emitCallResponse(command, response);

        return null;
    }

    private Integer createTopics(Admin admin, Port.Command command, Output output) throws InterruptedException {
        var newTopics = new LinkedList<NewTopic>();

        @SuppressWarnings("unchecked")
        var newTopicTuples = (Iterable<Object[]>) command.args()[0];
        for (var newTopicTuple : newTopicTuples) {
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
            output.emitCallResponse(command, Erlang.error(Erlang.binary(ErrorUtils.getMessage(e))));
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
            output.emitCallResponse(command, Erlang.error(Erlang.binary(ErrorUtils.getMessage(e))));
        }

        return null;
    }

    @FunctionalInterface
    interface Handler {
        Integer handle(Admin admin, Port.Command command, Output output) throws Exception;
    }
}
