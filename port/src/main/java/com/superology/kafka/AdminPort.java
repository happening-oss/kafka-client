package com.superology.kafka;

import java.util.*;
import java.util.concurrent.*;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.*;

import com.ericsson.otp.erlang.*;

/*
 * Exposes the {@link Admin} interface to Elixir.
 */
public class AdminPort implements Port {
  public static void main(String[] args) {
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");
    PortDriver.run(args, new AdminPort());
  }

  private Map<String, Handler> dispatchMap = Map.ofEntries(
      Map.entry("describe_topics", this::describeTopics),
      Map.entry("list_topics", this::listTopics));

  @Override
  public void run(PortWorker worker, PortOutput output, Object[] args) throws Exception {
    @SuppressWarnings("unchecked")
    var props = mapToProperties((Map<Object, Object>) args[0]);

    try (var admin = Admin.create(props)) {
      while (true) {
        var command = worker.take();

        if (command.name().equals("stop"))
          return;

        dispatchMap.get(command.name()).handle(admin, command, output);
      }
    }
  }

  private void listTopics(Admin admin, Port.Command command, PortOutput output)
      throws InterruptedException, ExecutionException {
    output.emitCallResponse(
        command,
        Erlang.toList(
            admin.listTopics().names().get(),
            name -> new OtpErlangBinary(name.getBytes())));
  }

  private void describeTopics(Admin admin, Port.Command command, PortOutput output)
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
            return new AbstractMap.SimpleEntry<>(topic, partitions);
          });

      response = Erlang.ok(map);
    } catch (ExecutionException e) {
      response = Erlang.error(new OtpErlangBinary(e.getCause().getMessage().getBytes()));
    }

    output.emitCallResponse(command, response);
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
    void handle(Admin admin, Port.Command command, PortOutput output) throws Exception;
  }
}
