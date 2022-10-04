package com.superology.kafka.admin;

import java.util.*;
import java.util.concurrent.*;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.*;

import com.ericsson.otp.erlang.*;

import com.superology.kafka.port.*;

public class Main implements Port {
  public static void main(String[] args) {
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");
    Driver.run(args, new Main());
  }

  private Map<String, Handler> dispatchMap = Map.ofEntries(
      Map.entry("stop", this::stop),
      Map.entry("describe_topics", this::describeTopics),
      Map.entry("list_topics", this::listTopics),
      Map.entry("list_end_offsets", this::listEndOffsets),
      Map.entry("list_consumer_group_offsets", this::listConsumerGroupOffsets),
      Map.entry("create_topics", this::createTopics),
      Map.entry("delete_topics", this::deleteTopics));

  @Override
  public int run(Worker worker, Output output, Object[] args) throws Exception {
    @SuppressWarnings("unchecked")
    var props = mapToProperties((Map<Object, Object>) args[0]);

    try (var admin = Admin.create(props)) {
      while (true) {
        var command = worker.take();
        var exitCode = dispatchMap.get(command.name()).handle(admin, command, output);
        if (exitCode != null)
          return exitCode;
      }
    }
  }

  private Integer stop(Admin admin, Port.Command command, Output output) {
    return 0;
  }

  private Integer listTopics(Admin admin, Port.Command command, Output output)
      throws InterruptedException, ExecutionException {
    output.emitCallResponse(
        command,
        Erlang.toList(
            admin.listTopics().names().get(),
            name -> new OtpErlangBinary(name.getBytes())));

    return null;
  }

  private Integer describeTopics(Admin admin, Port.Command command, Output output)
      throws InterruptedException {
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
                partition -> new OtpErlangInt(partition.partition()));
            return Erlang.mapEntry(topic, partitions);
          });

      response = Erlang.ok(map);
    } catch (ExecutionException e) {
      response = Erlang.error(new OtpErlangBinary(e.getCause().getMessage().getBytes()));
    }

    output.emitCallResponse(command, response);

    return null;
  }

  private Integer listEndOffsets(Admin admin, Port.Command command, Output output)
      throws InterruptedException {

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
          entry -> {
            return Erlang.mapEntry(
                Erlang.tuple(
                    new OtpErlangBinary(entry.getKey().topic().getBytes()),
                    new OtpErlangInt(entry.getKey().partition())),
                new OtpErlangLong(entry.getValue().offset()));

          });
      response = Erlang.ok(map);
    } catch (ExecutionException e) {
      response = Erlang.error(new OtpErlangBinary(e.getCause().getMessage().getBytes()));
    }

    output.emitCallResponse(command, response);

    return null;
  }

  private Integer listConsumerGroupOffsets(Admin admin, Port.Command command, Output output)
      throws InterruptedException {
    var topicPartitions = new LinkedList<TopicPartition>();

    for (@SuppressWarnings("unchecked")
    var topicPartitionTuple : ((Iterable<Object[]>) command.args()[1])) {
      var topicPartition = new TopicPartition((String) topicPartitionTuple[0], (int) topicPartitionTuple[1]);
      topicPartitions.add(topicPartition);
    }

    var options = new ListConsumerGroupOffsetsOptions();
    options.topicPartitions(topicPartitions);

    OtpErlangObject response;
    try {
      var map = Erlang.toMap(
          admin.listConsumerGroupOffsets((String) command.args()[0], options)
              .partitionsToOffsetAndMetadata()
              .get(),
          entry -> {
            var value = entry.getValue();
            OtpErlangObject offset = value == null ? Erlang.nil() : new OtpErlangLong(value.offset());

            return Erlang.mapEntry(
                Erlang.tuple(
                    new OtpErlangBinary(entry.getKey().topic().getBytes()),
                    new OtpErlangInt(entry.getKey().partition())),
                offset);
          });

      response = Erlang.ok(map);
    } catch (ExecutionException e) {
      response = Erlang.error(new OtpErlangBinary(e.getCause().getMessage().getBytes()));
    }

    output.emitCallResponse(command, response);

    return null;
  }

  private Integer createTopics(Admin admin, Port.Command command, Output output)
      throws InterruptedException {
    var newTopics = new LinkedList<NewTopic>();

    for (@SuppressWarnings("unchecked")
    var newTopicTuple : ((Iterable<Object[]>) command.args()[0])) {
      newTopics.add(new NewTopic(
          (String) newTopicTuple[0],
          Optional.ofNullable((Integer) newTopicTuple[1]),
          Optional.empty()));
    }

    try {
      admin.createTopics(newTopics).all().get();
      output.emitCallResponse(command, Erlang.ok());
    } catch (ExecutionException e) {
      output.emitCallResponse(command, Erlang.error(new OtpErlangBinary(e.getCause().getMessage().getBytes())));
    }

    return null;
  }

  private Integer deleteTopics(Admin admin, Port.Command command, Output output)
      throws InterruptedException {
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
