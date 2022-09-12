defmodule KafkaClient.Consumer.Processor do
  use GenServer
  require Logger
  alias KafkaClient.Consumer.Core

  def start_link(handler), do: GenServer.start_link(__MODULE__, handler)

  def handle_record(pid, record),
    do: GenServer.cast(pid, {:record, record})

  @impl GenServer
  def init(handler),
    do: {:ok, handler}

  @impl GenServer
  def handle_cast({:record, record}, handler) do
    Core.started_processing(record)

    try do
      :telemetry.span(
        [:kafka_client, :consumer, :record, :handler],
        %{},
        fn -> {handler.({:record, record}), Core.telemetry_meta(record)} end
      )
    catch
      kind, payload when kind != :exit ->
        Logger.error(Exception.format(kind, payload, __STACKTRACE__))
    end

    Core.ack(record)

    {:noreply, handler}
  end
end
