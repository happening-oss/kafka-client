defmodule KafkaClient.Consumer.PartitionProcessor do
  @moduledoc false

  # This module powers a GenServer which handles a single assigned partition. See
  # `KafkaClient.Consumer` for details.

  use GenServer
  require Logger
  alias KafkaClient.Consumer.Poller

  def start_link(handler), do: GenServer.start_link(__MODULE__, handler)

  def handle_record(pid, record),
    do: GenServer.cast(pid, {:record, record})

  @impl GenServer
  def init(handler),
    do: {:ok, handler}

  @impl GenServer
  def handle_cast({:record, record}, handler) do
    Poller.started_processing(record)

    try do
      :telemetry.span(
        [:kafka_client, :consumer, :record, :handler],
        %{},
        fn -> {handler.({:record, record}), Poller.telemetry_meta(record)} end
      )
    catch
      kind, payload when kind != :exit ->
        Logger.error(Exception.format(kind, payload, __STACKTRACE__))
    end

    Poller.ack(record)

    {:noreply, handler}
  end
end
