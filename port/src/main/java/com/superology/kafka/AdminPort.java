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

  @Override
  public void run(PortWorker worker, PortOutput output, Object[] args) throws Exception {
    @SuppressWarnings("unchecked")
    var props = mapToProperties((Map<Object, Object>) args[0]);

    try (var admin = Admin.create(props)) {
      while (true) {
        var command = worker.take();

        switch (command.name()) {
          case "stop":
            return;

          case "describe_topics":
            output.emitCallResponse(command, describeTopics(admin, command));
            break;

          case "list_topics":
            output.emitCallResponse(command, listTopics(admin));
            break;

          default:
            throw new RuntimeException("Invalid command: " + command.name());
        }
      }
    }
  }

  private OtpErlangList listTopics(Admin admin) throws InterruptedException, ExecutionException {
    return Erlang.toList(
        admin.listTopics().names().get(),
        name -> new OtpErlangBinary(name.getBytes()));
  }

  private OtpErlangTuple describeTopics(Admin admin, Port.Command command) throws InterruptedException {
    @SuppressWarnings("unchecked")
    var topics = (Collection<String>) command.args()[0];
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

      return Erlang.ok(map);
    } catch (ExecutionException e) {
      return Erlang.error(new OtpErlangBinary(e.getCause().getMessage().getBytes()));
    }
  }

  private Properties mapToProperties(Map<Object, Object> map) {
    // need to remove nulls, because Properties doesn't support them
    map.values().removeAll(Collections.singleton(null));
    var result = new Properties();
    result.putAll(map);
    return result;
  }
}
