defmodule KafkaClient.Producer do
  @doc """
  Kafka producer.

  This module wraps the functions from the [Java Producer interface]
  (https://javadoc.io/static/org.apache.kafka/kafka-clients/3.2.3/org/apache/kafka/clients/producer/KafkaProducer.html).
  """

  use KafkaClient.GenPort
  import Kernel, except: [send: 2]
  alias KafkaClient.GenPort

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
    user_producer_params = Keyword.get(opts, :producer_params, %{})

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
      nil,
      "producer.Main",
      [producer_params],
      Keyword.take(opts, ~w/name/a)
    )
  end

  @doc "Synchronously stops the producer process."
  @spec stop(GenServer.server(), timeout) :: :ok | {:error, :not_found}
  defdelegate stop(server, timeout \\ :infinity), to: GenPort

  @doc """
  Asynhronously sends a record.

  This function returns true as soon as the record has been enqueued in internal producer's state.
  The record will be sent sometime in the future, depending on the producer settings (see
  https://javadoc.io/static/org.apache.kafka/kafka-clients/3.2.3/org/apache/kafka/clients/producer/KafkaProducer.html
  for details).

  If the callback function is provided, it will be invoked after the send has been acknowledged by
  the brokers.

  If partition and/or timestamp values are not provided, they will be generated, as described in
  https://javadoc.io/static/org.apache.kafka/kafka-clients/3.2.3/org/apache/kafka/clients/producer/ProducerRecord.html.
  """
  @spec send(GenServer.server(), record, on_completion: (publish_result -> any)) :: :ok
  def send(server, record, opts \\ []),
    do: GenServer.call(server, {:send, record, opts})

  @doc """
  Synchronously sends a record.

  This function will block until the send has been acknowledged by the brokers.
  """
  @spec sync_send(GenServer.server(), record, timeout) :: publish_result
  def sync_send(server, record, timeout \\ :timer.seconds(5)) do
    server = GenServer.whereis(server)

    me = self()
    ref = make_ref()

    callback = &Kernel.send(me, {ref, &1})
    send(server, record, on_completion: callback)

    mref = Process.monitor(server)

    receive do
      {^ref, response} -> response
      {:DOWN, ^mref, :process, ^server, reason} -> exit(reason)
    after
      timeout -> exit(:timeout)
    end
  end

  @impl GenServer
  def init(_), do: {:ok, %{callbacks: %{}, next_ref: 0}}

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

    {ref, state} =
      if callback = Keyword.get(opts, :on_completion) do
        bin_ref = :erlang.term_to_binary(state.next_ref)
        callbacks = Map.put(state.callbacks, bin_ref, callback)
        {{:__binary__, bin_ref}, %{state | next_ref: state.next_ref + 1, callbacks: callbacks}}
      else
        {nil, state}
      end

    GenPort.command(GenPort.port(), :send, [transport_record, ref])
    {:reply, :ok, state}
  end

  @impl GenPort
  def handle_port_message({:on_completion, ref, payload}, state) do
    {callback, callbacks} = Map.pop!(state.callbacks, ref)
    callback.(payload)
    {:noreply, %{state | callbacks: callbacks}}
  end
end
