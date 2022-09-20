defmodule KafkaClient.Consumer.Poller do
  @moduledoc """
  The consumer poller loop powered by the Java Kafka client.

  This is the lower-level building block for implementing a kafka consumer. Typically you want to
  use more convenient modules, such as `KafkaClient.Consumer` or `KafkaClient.Consumer.Stream`.

  This module can be used when different process models or behaviours are required, such as
  `GenStage`, for example.

  This module is a lightweight wrapper around the Java port program, where most of the polling
  logic resides. Take a look at `ConsumerPoller` in Java for details. In a nutshell, this is a
  `GenServer` process which starts the port, and forwards the notifications emitted by the Java
  program to the client process, called _processor_. Implementing the processor is the
  responsibility of the client.

  ## Notifications

  Each message sent to the processor is a standard message in the shape of `{poller_pid,
  notification}`, where `notification` is of the type `t:notification/0`.

  ## Anonymous consumer vs consumer group

  If the `:group_id` option is not provided, or if it is set to `nil`, the poller will manually
  assign itself to all partitions of the desired topics, and poll messages from the beginning. The
  polled messages are not committed to Kafka.

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

  use GenServer
  require Logger
  alias KafkaClient.Consumer.Port

  @type option ::
          {:processor, pid}
          | {:servers, [String.t()]}
          | {:group_id, String.t() | nil}
          | {:topics, [String.t()]}
          | {:poll_duration, pos_integer}
          | {:commit_interval, pos_integer}
          | {:consumer_params, %{String.t() => any}}

  @type record :: %{
          optional(atom) => any,
          topic: topic,
          partition: partition,
          offset: non_neg_integer(),
          timestamp: pos_integer(),
          headers: [{String.t(), binary}],
          key: String.t(),
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
          {:assigned, [{topic, partition}]}
          | {:unassigned, [{topic, partition}]}
          | :caught_up
          | {:record, record}

  @type topic :: String.t()
  @type partition :: non_neg_integer

  @doc """
  Starts the poller process.

  Options:

    - `:processor` - the pid of the process which will receive the consumer notifications.
    - `:servers` - the list of the broker hosts, e.g. `["localhost:9092"]`.
    - `:group_id` - the name of the consumer group. Defaults to `nil` (anonymous consumer).
    - `:topics` - the list of topics to consume from (e.g. `["topic1", "topic2", ...]`).
    - `:poll_duration` - the duration of a single poll in milliseconds. Defaults to 10.
    - `:commit_interval` - the commit frequency in milliseconds. Defaults to 5000.
    - `:consumer_params` - a `String.t => any` map passed directly to the Java Kafka client.
      See https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html for details.
  """
  @spec start_link([option | {:processor, pid}]) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @doc "Synchronously stops the poller process."
  @spec stop(GenServer.server()) :: :ok
  def stop(pid), do: GenServer.stop(pid)

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
  Informs the poller that the record has been processed.

  An ack has a dual role: commits and backpressure.

  When the record is acknowledged, the poller will commit it to Kafka. The poller aggregates
  pending commits and periodically sends them to the broker.

  In addition, for each partition, the poller keeps track of in-flight records, i.e. records which
  have been polled, but not acknowledged. If the count or the total byte size of such records is
  too large, the poller will pause fetching from the corresponding partition.

  Therefore, it is important to invoke this function for each record received, even if its
  processing resulted in an exception. For most consistent behaviour, it's best to invoke this
  function after the record has been fully processed.
  """
  @spec ack(record) :: :ok
  def ack(record),
    do: Port.ack(record.port, record.topic, record.partition, record.offset)

  @doc "Returns the record fields used as a meta in telemetry events."
  @spec telemetry_meta(record) :: %{atom => any}
  def telemetry_meta(record), do: Map.take(record, ~w/topic partition offset timestamp/a)

  @impl GenServer
  def init(opts) do
    Process.flag(:trap_exit, true)
    processor = Keyword.fetch!(opts, :processor)
    port = Port.open(opts)
    Process.monitor(processor)
    {:ok, %{port: port, processor: processor}}
  end

  @impl GenServer
  def handle_info({port, {:data, data}}, %{port: port} = state) do
    data |> :erlang.binary_to_term() |> handle_port_message(state)
    {:noreply, state}
  end

  def handle_info({port, {:exit_status, status}}, %{port: port} = state) do
    Logger.error("port exited with status #{status}")
    {:stop, :port_crash, %{state | port: nil}}
  end

  def handle_info({:DOWN, _mref, :process, processor, reason}, %{processor: processor} = state),
    do: {:stop, reason, %{state | processor: nil}}

  @impl GenServer
  def terminate(_reason, state),
    do: if(state.port != nil, do: Port.close(state.port))

  defp handle_port_message(
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
      port: state.port,
      received_at: System.monotonic_time()
    }

    :telemetry.execute(
      [:kafka_client, :consumer, :record, :queue, :start],
      %{system_time: System.system_time(), monotonic_time: record.received_at},
      telemetry_meta(record)
    )

    notify_processor(state, {:record, record})
  end

  defp handle_port_message({:metrics, transfer_time, duration}, _state) do
    transfer_time = System.convert_time_unit(transfer_time, :nanosecond, :native)
    duration = System.convert_time_unit(duration, :nanosecond, :native)

    :telemetry.execute(
      [:kafka_client, :consumer, :port, :stop],
      %{
        system_time: System.system_time(),
        transfer_time: transfer_time,
        duration: duration
      },
      %{}
    )
  end

  defp handle_port_message(message, state), do: notify_processor(state, message)

  defp notify_processor(state, message), do: send(state.processor, {self(), message})
end
