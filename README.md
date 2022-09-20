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

## Examples

### Concurrent consuming

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

  handler: fn
    {:assigned, partitions} -> ...
    {:unassigned, partitions} -> ...

    # invoked only when group_id is nil or not provided
    :caught_up -> ...

    # Invoked in separate process for different partitions.
    # Records on the same partition are processed manually.
    {:record, record} -> ...
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
