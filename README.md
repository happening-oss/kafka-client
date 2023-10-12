# KafkaClient

Elixir wrapper around the [Apache Kafka Java client library](https://javadoc.io/doc/org.apache.kafka/kafka-clients/latest/index.html).

This wrapper is created to overcome deficiencies in the existing BEAM Kafka clients. Adapting those clients was considered, but it was estimated that implementing some of the features would be tricky to implement and maintain, since Kafka delegates a lot of complexity to the client, especially in the consumer part.

Therefore, this library takes a different approach. Instead of spending a lot of effort working on the BEAM client, we reuse the official Kafka client, which is developed, tested, and released together with Kafka itself. This gives us the easy access to the latest & greatest features without imposing a high development and maintenance burden.

## Features

- exposes Java client API to Elixir
- finer-grained concurrency on top of the consumer API
- at least once processing guarantees
- batching and backpressure
- topic consuming (consume all partitions of a topic)
- draining on shutdown

## Downsides

- only a small portion of API is exposed
- requires Java runtime
- adds a latency overhead due to extra hop between Java and BEAM
- still in early days, so bugs may exist, and the API might change radically

## Usage

The required tools and versions are specified in the .tool-versions file. You can use the [asdf version manager](https://asdf-vm.com/) to install them. You can also use newer versions in your own projects (unless there are some breaking changes).

You also need to run Kafka and Zookeeper. This project includes the docker-compose.yml file which starts these services.

This library is currently not published to hex.pm, so you need to add it as a github dependency:

```elixir
# in mix exs
defp deps do
  [
    {:kafka_client, github: "happening-oss/kafka-client"},
    ...
  ]
end
```

## Examples

### Producing

```elixir
KafkaClient.Producer.start_link(servers: ["localhost:9092"], name: :my_producer)

# fire-and-forget
KafkaClient.Producer.send(
  :my_producer,
  %{topic: "some_topic", key: "key", value: "value"}
)

# sync multi-produce
KafkaClient.Producer.sync_send(:my_producer, [record1, record2, ...])
|> Enum.each(&IO.inspect/1)
```

### Concurrent consuming

```elixir
KafkaClient.Consumer.start_link(
  servers: ["localhost:9092"],
  group_id: "mygroup",
  subscriptions: ["topic1", "topic2", ...],

  poll_duration: 10,
  commit_interval: :timer.seconds(5),

  # These parameters are passed directly to the Java client.
  # See https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html
  consumer_params: %{
    "heartbeat.interval.ms" => 100,
    "max.poll.interval.ms" => 1000,
  },

  handler: fn
    {:assigned, partitions} -> ...
    {:unassigned, partitions} -> ...

    # invoked only when group_id is nil or not provided
    :caught_up -> ...

    # Invoked in separate process for different partitions.
    # Records on the same partition are processed manually.
    {:records, records} -> ...
  end
)
```

### Stream

```elixir
# Anonymous consuming (no consumer group)
KafkaClient.Consumer.Stream.new([group_id: nil, ...])

# Stop once all the records have been processed. For this to work, it is important to ack each record.
|> Stream.take_while(&(&1 != :caught_up))

# Take only the record notifications (i.e. ignore assigned/unassigned).
|> Stream.filter(&match?({:record, record}, &1))

# Process each record
|> Enum.each(fn {:record, record} ->
  do_something_with(record)

  # don't forget to ack after the record is processed
  KafkaClient.Consumer.Poller.ack(record)
end)
```

See documentation of the following modules for detailed description:

- [KafkaClient.Consumer](lib/kafka_client/consumer.ex)
- [KafkaClient.Consumer.Poller](lib/kafka_client/consumer/poller.ex)
- [KafkaClient.Consumer.Stream](lib/kafka_client/consumer/stream.ex)
