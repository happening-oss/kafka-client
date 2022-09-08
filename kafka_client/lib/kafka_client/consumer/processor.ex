defmodule KafkaClient.Consumer.Processor do
  use GenServer
  require Logger

  def start_link(arg), do: GenServer.start_link(__MODULE__, arg)

  def handle_record(pid, offset, timestamp, payload, enqueued_at),
    do: GenServer.cast(pid, {:record, offset, timestamp, payload, enqueued_at})

  @impl GenServer
  def init(arg),
    do: {:ok, arg}

  @impl GenServer
  def handle_cast({:record, offset, timestamp, payload, enqueued_at}, state) do
    {parent, topic, partition, end_offset, handler, port} = state
    now = System.monotonic_time()

    record = %{
      topic: topic,
      partition: partition,
      offset: offset,
      timestamp: timestamp,
      payload: payload
    }

    telemetry_meta = %{topic: topic, partition: partition, offset: offset, timestamp: timestamp}

    :telemetry.execute(
      [:kafka_client, :consumer, :record, :queue, :stop],
      %{system_time: System.system_time(), monotonic_time: now, duration: now - enqueued_at},
      telemetry_meta
    )

    try do
      :telemetry.span(
        [:kafka_client, :consumer, :record, :handler],
        %{},
        fn -> {handler.({:record, record}), telemetry_meta} end
      )
    catch
      kind, payload when kind != :exit ->
        Logger.error(Exception.format(kind, payload, __STACKTRACE__))
    end

    Port.command(
      port,
      :erlang.term_to_binary({:ack, topic, partition, offset})
    )

    state =
      if end_offset != nil and offset + 1 >= end_offset do
        send(parent, {:caught_up, {topic, partition}})
        {parent, topic, partition, nil, handler, port}
      else
        state
      end

    {:noreply, state}
  end
end
