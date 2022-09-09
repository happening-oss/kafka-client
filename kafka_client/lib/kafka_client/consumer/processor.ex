defmodule KafkaClient.Consumer.Processor do
  use GenServer
  require Logger
  alias KafkaClient.Consumer.Port

  def start_link(arg), do: GenServer.start_link(__MODULE__, arg)

  def handle_record(pid, offset, timestamp, payload, enqueued_at),
    do: GenServer.cast(pid, {:record, offset, timestamp, payload, enqueued_at})

  @impl GenServer
  def init(arg),
    do: {:ok, arg}

  @impl GenServer
  def handle_cast({:record, offset, timestamp, payload, enqueued_at}, state) do
    now = System.monotonic_time()

    record = %{
      topic: state.topic,
      partition: state.partition,
      offset: offset,
      timestamp: timestamp,
      payload: payload
    }

    telemetry_meta = %{
      topic: state.topic,
      partition: state.partition,
      offset: offset,
      timestamp: timestamp
    }

    :telemetry.execute(
      [:kafka_client, :consumer, :record, :queue, :stop],
      %{system_time: System.system_time(), monotonic_time: now, duration: now - enqueued_at},
      telemetry_meta
    )

    try do
      :telemetry.span(
        [:kafka_client, :consumer, :record, :handler],
        %{},
        fn -> {state.handler.({:record, record}), telemetry_meta} end
      )
    catch
      kind, payload when kind != :exit ->
        Logger.error(Exception.format(kind, payload, __STACKTRACE__))
    end

    Port.ack(state.port, state.topic, state.partition, offset)

    state =
      if state.end_offset != nil and offset + 1 >= state.end_offset do
        send(state.parent, {:caught_up, {state.topic, state.partition}})
        %{state | end_offset: nil}
      else
        state
      end

    {:noreply, state}
  end
end
