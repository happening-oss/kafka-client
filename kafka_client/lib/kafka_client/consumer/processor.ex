defmodule KafkaClient.Consumer.Processor do
  use GenServer
  require Logger
  alias KafkaClient.Consumer.Port

  def start_link(handler), do: GenServer.start_link(__MODULE__, handler)

  def handle_record(pid, record, enqueued_at),
    do: GenServer.cast(pid, {:record, record, enqueued_at})

  @impl GenServer
  def init(handler),
    do: {:ok, handler}

  @impl GenServer
  def handle_cast({:record, record, enqueued_at}, handler) do
    now = System.monotonic_time()

    telemetry_meta = Map.take(record, ~w/topic partition offset timestamp/a)

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

    Port.ack(record.port, record.topic, record.partition, record.offset)

    {:noreply, handler}
  end
end
