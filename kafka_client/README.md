# KafkaClient

## Compilation

1. Install asdf version manager and add the following plugins: elixir, erlang, java, maven
2. asdf install
3. mix deps.get
4. mix compile

## Running

1. Make sure there is a local kafka instance running on the port 9092.
2. `iex -S mix run -e TestConsumer.start`
3. In a separate terminal window publish some messages on the topic `mytopic`, and check that they are printed in the consumer window.

## Example usage

```elixir
KafkaClient.Consumer.start_link(
  servers: ["localhost:9092"],
  group_id: "mygroup",
  topics: ["topic1", "topic2", ...],

  poll_duration: 10,
  commit_interval: :timer.seconds(5),

  # These parameters are passed directly to the Java client.
  # See https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html
  consumer_params: %{
    "heartbeat.interval.ms" => 100,
    "max.poll.interval.ms" => 1000,
  },

  handler: &handle_message/1
)
```

## The handler function

The handler function is invoked with the following arguments:

- `{:assigned, partitions}` - Invoked when partitions are assigned to the consumer.
- `{:unassigned, partitions}` - Invoked when the partitions have been revoked from the consumer. This notification is emitted only if the consumer is in a consumer group.
- `:caught_up` - Invoked when the consumer has caught up with the initial partition tails (aka high watermarks), at the time the consumer first connected to the broker. This notification is emitted only if the consumer is not in a consumer group.
- `{:record, record}` - Invoked to process the record. The `record` is a map with the following spec:

    ```elixir
    %{
      topic: String.t(),
      partition: non_neg_integer(),
      offset: non_neg_integer(),
      timestamp: pos_integer(),
      payload: binary
    }
    ```

## Anonymous consumer vs consumer group

If the `:group_id` option is not provided, or if it is set to `nil`, the consumer will manually assign itself to all partitions of the desired topics, and poll messages from the beginning. The polled messages are not committed to Kafka.

If the `:group_id` options is provided and not `nil`, the consumer will subscribe to the desired topics. The partitions will be automatically assigned to the consumer, as a part of the rebalance.

## Concurrency consideration

Messages on the same partition are processed sequentially. Messages on different partitions are processed concurrently. Internally, the consumer maintains one long-running process per each assigned partition. This process is started when the partition is assigned to the consumer, and it is terminated if the partition is unassigned.

If the handler is invoked with the argument `{:record, record}`, the invocation takes place inside the partition process. All other handler invocations take place inside the main consumer process (which is the parent of the partitions processes). Avoid long processing, exceptions, and exits from these other handler invocations, because they might block the consumer or take it down completely. The `{:record, record}` handler invocation may run arbitrarily long, and it may safely throw an exception (see [Processing guarantees](#processing-guarantees)).

## Processing guarantees

The consumer provides at-least-once processing guarantees, i.e. it is guaranteed that the `handler({:record, record})` invocation will finish at least once for each record. After the handler function finishes, the consumer will commit it to Kafka. This will also happen if the handler function throws an exception. If you wish to handle the exception yourself, e.g. by retrying or republishing the message, you must catch the exception inside the handler function.

If you wish to commit the record before it is processed, you can asynchronously send the record payload to another process, e.g. via `send` or `cast`, and then return from the handler function immediately. Alternatively, you can spawn another process to handle the message. This will change the processing guarantees to at-most-once, since it is possible that a record is committed, but never fully processed (e.g. the machine is taken down after the commits are flushed, but before the handler finishes).

If the handler is spawning processes, they must be started somewhere else in the application supervision tree, not as direct children of the process where the handler is running (the partition process). For example, if you wish to handle the message asynchronously in a task, use `Task.Supervisor.start_child`, not `Task.start_link`. The latter may cause unexpected `:EXIT` messages, in which case the entire consumer will terminate. On the other hand, using `Task.async` with `Task.await` in the handler is fine, as long as you can be certain that tasks won't crash, or that the `await` won't time out.


### Shutdown and unassigned behaviour

For efficiency reasons, the consumer aggregates pending commits, and submits them to the broker periodically. If a partition is unassigned, the consumer will attempt to flush all pending commits. The related partition process is then forcefully terminated. Likewise, on shutdown, the consumer will attempt to flush all pending commits on all topics, and forcefully terminate all partition processes. The consumer won't wait for the currently running handlers to finish, nor will it attempt to drain the current buffer (i.e. records already polled from Kafka). This approach reduces (but doesn't remove) the chance of a record being processed more than once.

## Load control

The consumer keeps the unprocessed polled records in a collection of queues, one queue per each assigned partition. If some partition queue becomes full, the consumer pauses fetching the records for the given partition.

A partition queue is considered full if it has 1000 entries, or if the total size of the messages in the queue exceeds 1 megabyte. Note that these limits are soft. It is possible to significantly exceed them, depending on the amount and the size of messages taken in the next server poll. Some settings that may be used to mitigate this are:

- [max.poll.records](https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html#consumerconfigs_max.poll.records)
- [fetch.max.bytes](https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html#consumerconfigs_fetch.max.bytes)
- [max.partition.fetch.bytes](https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html#consumerconfigs_max.partition.fetch.bytes)

The consumer will resume polling from a paused partition once the queue drops to 500 messages or 500 kilobytes in size.

## Telemetry

The consumer emits the following measurements via telemetry:

- The total amount of time the record spent in the Java program (`kafka_client.consumer.port.stop.duration`)
- The I/O time it took to transfer the record to Elixir (as observed in Java) (`kafka_client.consumer.port.stop.transfer_time`)
- The total amount of time the record was queued on the Elixir side, before the handler was invoked (`kafka_client.consumer.record.queue.stop.duration`)
- Duration of the handler (`kafka_client.consumer.record.handler.stop.duration` and `kafka_client.consumer.record.handler.exception.duration`)

These durations can be obtained from the `measurements` map of the corresponding telemetry events (e.g. `[:kafka_client, :consumer, :port, :stop]`). All durations are provided in native time units. You can convert them to the desired unit with `System.convert_time_unit`.
