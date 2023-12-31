defmodule KafkaClient.Consumer.Poller do
  @moduledoc """
  The consumer poller loop powered by the Java Kafka client.

  This is the lower-level building block for implementing a kafka consumer. Typically you want to
  use more convenient modules, such as `KafkaClient.Consumer` or `KafkaClient.Consumer.Stream`.

  This module can be used when different process models or behaviours are required, such as
  `GenStage`, for example.

  This module is a lightweight wrapper around the Java port program, where most of the polling
  logic resides. Take a look at `consumer.Main` in Java for details. In a nutshell, this is a
  `GenServer` process which starts the port, and forwards the notifications emitted by the Java
  program to the client process, called _processor_. Implementing the processor is the
  responsibility of the client.

  ## Notifications

  Each message sent to the processor is a standard message in the shape of `{poller_pid,
  notification}`, where `notification` is of the type `t:notification/0`.

  ## Anonymous consumer vs consumer group

  If the `:group_id` option is not provided, or if it is set to `nil`, the poller will manually
  assign itself to the desired subscriptions. Anonymous consumer will not commit the acknowledged
  messages to kafka.

  If the `:group_id` options is provided and not `nil`, the poller will subscribe to the desired
  topics. The partitions will be automatically assigned to the poller, as a part of the rebalance.

  ## Load control

  The poller keeps the unprocessed polled records in a collection of queues, one queue per each
  assigned partition. If some partition queue becomes full, the poller pauses fetching the records
  for the given partition.

  A partition queue is considered full if it has 1000 entries, or if the total size of the messages
  in the queue exceeds 1 megabyte. Note that these limits are soft. It is possible to significantly
  exceed them, depending on the amount and the size of messages taken in the next server poll. Some
  settings that may be used to mitigate this are:

  - [max.poll.records](https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html#consumerconfigs_max.poll.records)
  - [fetch.max.bytes](https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html#consumerconfigs_fetch.max.bytes)
  - [max.partition.fetch.bytes](https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html#consumerconfigs_max.partition.fetch.bytes)

  The poller will resume polling from a paused partition once the queue drops to 500 messages or
  500 kilobytes in size.

  ## Telemetry

  The consumer emits the following measurements via telemetry:

  - The total amount of time the record spent in the Java program
    (`kafka_client.consumer.port.stop.duration`)
  - The I/O time it took to transfer the record to Elixir (as observed in Java)
    (`kafka_client.consumer.port.stop.transfer_time`)
  - The total amount of time the record was queued on the Elixir side, before the handler was
    invoked (`kafka_client.consumer.record.queue.stop.duration`)

  These durations can be obtained from the `measurements` map of the corresponding telemetry events
  (e.g. `[:kafka_client, :consumer, :port, :stop]`). All durations are provided in native time
  units. You can convert them to the desired unit with `System.convert_time_unit`.

  ## Client responsibilities

  The client implementation is responsible for handling notification messages sent by the poller
  process.

  For every `{:record, record}` notification, the client must inform the poller that it's starting
  to process the record (via `started_processing/1`), and acknowledge that the record has been
  processed (via `ack/1`). Failing to send an ack may cause the poller to stop fetching
  new records, due to  backpressure mechanism.

  The processor should stop the poller when it's not needed anymore. This can be done either by
  invoking `stop/1`, or sending an exit signal (via `Process.exit/2`), if the processor is the
  direct parent of the poller.

  The processor should monitor the poller, and stop itself if the poller stops. The poller and its
  processor are tightly coupled, so they should be restarted together. The easiest way to achieve
  this is to run the poller as the direct child of the processor (as done by `KafkaClient.Consumer`
  and `KafkaClient.Consumer.Stream`), or to run both processes as the children of a `one_for_all`
  supervisor.
  """

  use KafkaClient.GenPort
  require Logger
  alias KafkaClient.GenPort

  @type option ::
          {:processor, pid}
          | {:servers, [String.t()]}
          | {:group_id, String.t() | nil}
          | {:subscriptions, [subscription]}
          | {:poll_duration, pos_integer}
          | {:commit_interval, pos_integer}
          | {:consumer_params, %{String.t() => any}}

  @typedoc """
  Represents a consumer subscription.

  A subscription can be a topic name, a `{topic, partition}` pair, or a `{topic, partition, position}` triplet.

  If the consumer is in a consumer group, only the topic name will be used, while other fields are
  ignored. The consumer will subscribe to the given topic, and the partitions will be auto-assigned
  via rebalance.

  An anonymous consumer will self-assign to the desired topic-partitions (or all partitions if only
  the topic name is provided). The consumer will also jump to the given position (if provided).
  """
  @type subscription ::
          KafkaClient.topic()
          | KafkaClient.topic_partition()
          | {KafkaClient.topic(), KafkaClient.partition(), subscription_position}

  @type subscription_position ::
          KafkaClient.offset()
          | {:offset, KafkaClient.offset()}
          | {:timestamp, KafkaClient.timestamp()}

  @type record :: %{
          optional(atom) => any,
          topic: KafkaClient.topic(),
          partition: KafkaClient.partition(),
          offset: KafkaClient.offset(),
          timestamp: KafkaClient.timestamp(),
          headers: [{String.t(), binary}],
          key: binary,
          value: binary
        }

  @typedoc """
  A notification sent to the processor.

  The notification is sent in the shape of `{poller_pid, notification}`, and it can be one of the
  following:

      - `{:assigned, partitions}` - partitions are assigned to the poller
      - `{:unassigned, partitions}` - partitions are unassigned from the consumer
      - `caught_up` - an anonymous poller (with `group_id` set to `nil`) processed all records on
        all partitions that were present at the time it connected to the broker(s)
      - `{:record, record}` - a record is polled
  """
  @type notification ::
          {:assigned, [KafkaClient.topic_partition()]}
          | {:unassigned, [KafkaClient.topic_partition()]}
          | :caught_up
          | {:record, record}

  @doc """
  Starts the poller process.

  Options:

    - `:processor` - the pid of the process which will receive the consumer notifications.
    - `:servers` - the list of the broker hosts, e.g. `["localhost:9092"]`.
    - `:group_id` - the name of the consumer group. Defaults to `nil` (anonymous consumer).
    - `:subscriptions` - the list of subscriptions to consume from (e.g. `["topic1", "topic2", ...]`).
      See `t:subscription/0` for details.
    - `:poll_duration` - the duration of a single poll in milliseconds. Defaults to 10.
    - `:commit_interval` - the commit frequency in milliseconds. Defaults to 5000.
    - `:consumer_params` - a `String.t => any` map passed directly to the Java Kafka client.
      See https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html for details.
  """
  @spec start_link([option | {:processor, pid} | {:name, GenServer.name()}]) ::
          GenServer.on_start()
  def start_link(opts) do
    servers = Keyword.fetch!(opts, :servers)
    subscriptions = opts |> Keyword.fetch!(:subscriptions) |> Enum.map(&full_subscription/1)
    group_id = Keyword.get(opts, :group_id)
    user_consumer_params = Keyword.get(opts, :consumer_params, %{})

    poller_properties = %{
      "poll_duration" => Keyword.get(opts, :poll_duration, 10),
      "commit_interval" => Keyword.get(opts, :commit_interval, :timer.seconds(5))
    }

    consumer_params =
      %{"auto.offset.reset" => "earliest"}
      |> Map.merge(user_consumer_params)
      # non-overridable params
      |> Map.merge(%{
        "bootstrap.servers" => Enum.join(servers, ","),
        "group.id" => group_id,
        "enable.auto.commit" => false,
        "key.deserializer" => "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        "value.deserializer" => "org.apache.kafka.common.serialization.ByteArrayDeserializer"
      })

    GenPort.start_link(
      __MODULE__,
      Keyword.fetch!(opts, :processor),
      "consumer.Main",
      [consumer_params, subscriptions, poller_properties],
      Keyword.take(opts, ~w/name/a)
    )
  end

  @doc "Synchronously stops the poller process."
  @spec stop(GenServer.server(), timeout) :: :ok | {:error, :not_found}
  defdelegate stop(server, timeout \\ :infinity), to: GenPort

  @doc """
  Informs the poller that the record processing has been started.

  This function should be invoked by the processor as soon as it receives the record, to emit the
  queue stop telemetry event.
  """
  @spec started_processing(record) :: :ok
  def started_processing(record) do
    now = System.monotonic_time()

    :telemetry.execute(
      [:kafka_client, :consumer, :record, :queue, :stop],
      %{
        system_time: System.system_time(),
        monotonic_time: now,
        duration: now - record.received_at
      },
      telemetry_meta(record)
    )
  end

  @doc """
  Informs the poller that the records have been processed.

  An ack has a dual role: commits and backpressure.

  When the record is acknowledged, the poller will commit it to Kafka. The poller aggregates
  pending commits and periodically sends them to the broker.

  In addition, for each partition, the poller keeps track of in-flight records, i.e. records which
  have been polled, but not acknowledged. If the count or the total byte size of such records is
  too large, the poller will pause fetching from the corresponding partition.

  Therefore, it is important to invoke this function for each record received, even if its
  processing resulted in an exception. For most consistent behaviour, it's best to invoke this
  function after the record has been fully processed.

  This function accepts a single record as well as a list of records.
  """
  @spec ack(record | [record]) :: :ok
  def ack([]), do: :ok

  def ack(record_or_records) do
    records = List.wrap(record_or_records)
    port = hd(records).port
    arg = Enum.map(records, &[&1.topic, &1.partition, &1.offset])
    GenPort.command(port, :ack, [arg])
  end

  @doc "Returns the record fields used as a meta in telemetry events."
  @spec telemetry_meta(record) :: %{atom => any}
  def telemetry_meta(record), do: Map.take(record, ~w/topic partition offset timestamp/a)

  @impl GenServer
  def init(processor) do
    Process.monitor(processor)
    {:ok, %{processor: processor}}
  end

  @impl GenServer
  def handle_info({:DOWN, _mref, :process, processor, reason}, %{processor: processor} = state),
    do: {:stop, reason, %{state | processor: nil}}

  @impl GenPort
  def handle_port_message(
        {:record, topic, partition, offset, timestamp, headers, key, value},
        state
      ) do
    record = %{
      topic: topic,
      partition: partition,
      offset: offset,
      timestamp: timestamp,
      headers: headers,
      key: key,
      value: value,
      port: GenPort.port(),
      received_at: System.monotonic_time()
    }

    :telemetry.execute(
      [:kafka_client, :consumer, :record, :queue, :start],
      %{system_time: System.system_time(), monotonic_time: record.received_at},
      telemetry_meta(record)
    )

    notify_processor(state, {:record, record})

    {:noreply, state}
  end

  def handle_port_message(message, state) do
    notify_processor(state, message)
    {:noreply, state}
  end

  defp notify_processor(state, message), do: send(state.processor, {self(), message})

  defp full_subscription(topic) when is_binary(topic), do: full_subscription({topic, -1})
  defp full_subscription({topic, partition}), do: full_subscription({topic, partition, nil})
  defp full_subscription({topic, :all, position}), do: full_subscription({topic, -1, position})

  defp full_subscription({topic, partition, position}) do
    {position_type, position} =
      case position do
        nil -> {nil, nil}
        offset when is_integer(offset) -> {0, offset}
        {:offset, offset} -> {0, offset}
        {:timestamp, timestamp} -> {1, timestamp}
      end

    {topic, partition, position_type, position}
  end
end
