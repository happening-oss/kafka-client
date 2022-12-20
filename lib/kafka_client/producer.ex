defmodule KafkaClient.Producer do
  @doc """
  Kafka producer.

  This module wraps the functions from the [Java Producer interface]
  (https://javadoc.io/static/org.apache.kafka/kafka-clients/3.2.3/org/apache/kafka/clients/producer/KafkaProducer.html).

  ## Basic usage

  Start the producer in the supervision tree. Typically it suffices to have one, globally registered producer:

      children = [
        {KafkaClient.Producer, servers: ["localhost:9092"], name: :my_producer, producer_params: %{}},
        ...
      ]
      Supervisor.start_children(children, ...)

      # fire-and-forget send
      record = %{topic: "some_topic", key: "key", value: "value"}
      KafkaClient.Producer.send(:my_producer, record)

      # sending with an ack callback
      KafkaClient.Producer.send(
        :my_producer,
        record,
        on_completion: fn
          {:ok, partition, offset, timestamp} -> ...
          {:error, reason} -> ...
        end
      )

  `producer_params` is a map that is passed directly to Java's producer (https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html),
  with exception of "internal.buffer_size" that denotes internal buffer to the producer.
  """

  use KafkaClient.GenPort
  import Kernel, except: [send: 2]
  alias KafkaClient.GenPort

  @default_buffer_size 256 * 1024 * 1024

  @type record :: %{
          optional(:partition) => KafkaClient.partition() | nil,
          optional(:timestamp) => KafkaClient.timestamp() | nil,
          optional(:key) => binary | nil,
          optional(:value) => binary | nil,
          optional(:headers) => [{String.t(), binary}],
          topic: KafkaClient.topic()
        }

  @type publish_result ::
          {:ok, KafkaClient.partition(), KafkaClient.offset(), KafkaClient.timestamp()}
          | {:error, String.t()}

  @doc """
  Starts the producer process.

  On termination, the producer will attempt to flush the buffered messages to the brokers.
  """
  @spec start_link(
          servers: [String.t()],
          producer_params: %{String.t() => any},
          name: GenServer.name()
        ) ::
          GenServer.on_start()
  def start_link(opts) do
    servers = Keyword.fetch!(opts, :servers)

    producer_params = Keyword.get(opts, :producer_params, %{})

    {internal_buffer_size, user_producer_params} =
      Map.pop(producer_params, "internal.buffer_size", @default_buffer_size)

    producer_params =
      Map.merge(
        user_producer_params,
        %{
          "bootstrap.servers" => Enum.join(servers, ","),
          "key.serializer" => "org.apache.kafka.common.serialization.ByteArraySerializer",
          "value.serializer" => "org.apache.kafka.common.serialization.ByteArraySerializer"
        }
      )

    GenPort.start_link(
      __MODULE__,
      [max_buffer_size: internal_buffer_size],
      "producer.Main",
      [producer_params],
      Keyword.take(opts, ~w/name/a)
    )
  end

  @doc "Synchronously stops the producer process."
  @spec stop(GenServer.server(), timeout) :: :ok | {:error, :not_found}
  defdelegate stop(server, timeout \\ :infinity), to: GenPort

  @doc """
  Asynhronously sends a record to the producer.

  This function returns :ok as soon as the record has been enqueued in internal producer's state,
  or {:error, :buffer_size_exceeded} if internal buffer is full - clients should back-off and retry
  when this happens. Overriding default buffer size of 256Mib is done by setting `internal.buffer_size`
  in producer settings when spawning producer.

  The record will be sent sometime in the future, depending on the producer settings (see
  https://javadoc.io/static/org.apache.kafka/kafka-clients/3.2.3/org/apache/kafka/clients/producer/KafkaProducer.html
  for details).

  If the callback function is provided, it will be invoked after the send has been acknowledged by
  the brokers, according to the `"acks"` producer configuration
  (https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html#producerconfigs_acks).

  The callback function is invoked inside the producer process. Therefore make sure to keep the
  function short and crash-free. Typically it's best to send the result to another process, or
  store it in an ETS table, and finish quickly.

  If partition and/or timestamp values are not provided, they will be generated, as described in
  https://javadoc.io/static/org.apache.kafka/kafka-clients/3.2.3/org/apache/kafka/clients/producer/ProducerRecord.html.
  """
  @spec send(GenServer.server(), record, on_completion: (publish_result -> any)) ::
          :ok | {:error, :buffer_size_exceeded}
  def send(server, record, opts \\ []),
    do: GenServer.call(server, {:send, record, opts})

  @doc """
  Synchronously produces multiple records.

  This function sends all the records to the broker. The result is a stream of returned ack
  messages where each element is in the form of `{:ok, record} | {:error, record, reason}`.

  If the record is successfully published, the `record` field in the result is the input map
  enriched with the server-returned meta: offset, timestamp, partition.

  This function will perform poorly for a larger number of input records, because the mailbox of
  the caller process will be flooded. In such cases it's better to use `send/3`, and send the ack
  results to another process (not the caller).
  """
  @spec sync_send(GenServer.server(), Enumerable.t(), timeout) :: Enumerable.t()
  def sync_send(server, records, timeout \\ :timer.seconds(5)) do
    server = GenServer.whereis(server)

    caller = self()
    batch_ref = make_ref()

    sent_records =
      Map.new(
        records,
        fn record ->
          record_ref = make_ref()
          send(server, record, on_completion: &Kernel.send(caller, {batch_ref, record_ref, &1}))
          {record_ref, record}
        end
      )

    Stream.resource(
      fn ->
        {
          sent_records,
          Process.monitor(server),
          if(timeout != :infinity, do: Process.send_after(self(), {batch_ref, :timeout}, timeout))
        }
      end,
      fn {sent_records, mref, timer} ->
        if map_size(sent_records) == 0 do
          {:halt, {%{}, mref, timer}}
        else
          receive do
            {^batch_ref, :timeout} ->
              exit(:timeout)

            {:DOWN, ^mref, :process, ^server, reason} ->
              exit(reason)

            {^batch_ref, record_ref, response} ->
              {record, sent_records} = Map.pop!(sent_records, record_ref)

              emitted_element =
                case response do
                  {:ok, partition, offset, timestamp} ->
                    # if "acks" is set to 0, partition will be -1, so we convert it to `nil` in this case
                    partition = with -1 <- partition, do: nil

                    {:ok,
                     Map.merge(
                       record,
                       %{partition: partition, offset: offset, timestamp: timestamp}
                     )}

                  {:error, reason} ->
                    {:error, record, reason}
                end

              {[emitted_element], {sent_records, mref, timer}}
          end
        end
      end,
      fn {_sent_records, mref, timer} ->
        Process.demonitor(mref, [:flush])

        if timer != nil do
          Process.cancel_timer(timer)

          receive do
            {^batch_ref, :timeout} -> :ok
          after
            0 -> nil
          end
        end
      end
    )
  end

  @doc """
    Get the full set of internal metrics maintained by the producer.
  """
  @spec metrics(GenServer.server()) :: %{String.t() => integer() | float() | String.t()}
  def metrics(server) do
    GenPort.call(server, :metrics)
  end

  @impl GenServer
  def init(params) do
    {:ok, %{callbacks: %{}, next_ref: 0, producer_params: params, buffer_size: 0}}
  end

  @impl GenServer
  def handle_call({:send, record, opts}, _from, state) do
    transport_record =
      %{key: nil, value: nil, headers: []}
      |> Map.merge(record)
      |> Map.update!(:key, &{:__binary__, &1})
      |> Map.update!(:value, &{:__binary__, &1})
      |> Map.update!(
        :headers,
        &Enum.map(&1, fn {key, value} -> {key, {:__binary__, value}} end)
      )

    transport_record_size = :erlang.external_size(transport_record)

    current_buffer_size = state.buffer_size + transport_record_size

    if current_buffer_size < state.producer_params[:max_buffer_size] do
      {ref, state} =
        if callback = Keyword.get(opts, :on_completion) do
          bin_ref = :erlang.term_to_binary(state.next_ref)
          callbacks = Map.put(state.callbacks, bin_ref, {callback, transport_record_size})

          {{:__binary__, bin_ref},
           %{
             state
             | next_ref: state.next_ref + 1,
               callbacks: callbacks,
               buffer_size: current_buffer_size
           }}
        else
          {nil, state}
        end

      GenPort.command(GenPort.port(), :send, [transport_record, ref])
      {:reply, :ok, state}
    else
      {:reply, {:error, :buffer_size_exceeded}, state}
    end
  end

  @impl GenPort
  def handle_port_message({:on_completion, ref, payload}, state) do
    {{callback, record_size}, callbacks} = Map.pop!(state.callbacks, ref)
    callback.(payload)

    {:noreply, %{state | callbacks: callbacks, buffer_size: state.buffer_size - record_size}}
  end
end
