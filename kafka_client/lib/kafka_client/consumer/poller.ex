defmodule KafkaClient.Consumer.Poller do
  use GenServer
  require Logger
  alias KafkaClient.Consumer.Port

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  def ack(record),
    do: Port.ack(record.port, record.topic, record.partition, record.offset)

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

  def telemetry_meta(record), do: Map.take(record, ~w/topic partition offset timestamp/a)

  def stop(pid), do: GenServer.stop(pid)

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

  defp handle_port_message({:record, topic, partition, offset, timestamp, payload}, state) do
    record = %{
      topic: topic,
      partition: partition,
      offset: offset,
      timestamp: timestamp,
      payload: payload,
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
