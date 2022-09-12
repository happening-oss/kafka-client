defmodule KafkaClient.Consumer do
  use Parent.GenServer
  require Logger
  alias KafkaClient.Consumer.Core

  def start_link(opts), do: Parent.GenServer.start_link(__MODULE__, opts)

  @impl GenServer
  def init(opts) do
    {handler, opts} = Keyword.pop!(opts, :handler)

    {:ok, _subscriber} =
      Parent.start_child(
        {Core, Keyword.put(opts, :subscriber, self())},
        id: :core,
        restart: :temporary,
        ephemeral?: true
      )

    {:ok, %{handler: handler, port: nil}}
  end

  @impl GenServer
  def handle_info({:port_started, port}, state),
    do: {:noreply, %{state | port: port}}

  def handle_info(:caught_up, state) do
    state.handler.(:caught_up)
    {:noreply, state}
  end

  def handle_info({:assigned, partitions} = event, state) do
    start_processors(state, partitions)
    state.handler.(event)
    {:noreply, state}
  end

  def handle_info({:unassigned, partitions} = event, state) do
    Enum.each(partitions, &Parent.shutdown_child({:processor, &1}))
    state.handler.(event)
    {:noreply, state}
  end

  def handle_info({:record, record}, state) do
    now = System.monotonic_time()

    :telemetry.execute(
      [:kafka_client, :consumer, :record, :queue, :start],
      %{system_time: System.system_time(), monotonic_time: now},
      Map.take(record, ~w/topic partition offset timestamp/a)
    )

    {:ok, pid} = Parent.child_pid({:processor, {record.topic, record.partition}})
    KafkaClient.Consumer.Processor.handle_record(pid, record, now)

    {:noreply, state}
  end

  def handle_info({:metrics, transfer_time, duration}, state) do
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

    {:noreply, state}
  end

  def handle_info({:committed, _offsets} = event, state) do
    state.handler.(event)
    {:noreply, state}
  end

  @impl Parent.GenServer
  def handle_stopped_children(children, state) do
    crashed_children = Map.keys(children)
    {:stop, {:children_crashed, crashed_children}, state}
  end

  defp start_processors(state, partitions) do
    Enum.each(
      partitions,
      fn {topic, partition} ->
        {:ok, _pid} =
          Parent.start_child(
            {KafkaClient.Consumer.Processor, state.handler},
            id: {:processor, {topic, partition}},
            restart: :temporary,
            ephemeral?: true,

            # We want to kill the processor immediately, and stop any currently running processor,
            # even if the processor is trapping exits.
            shutdown: :brutal_kill
          )
      end
    )
  end
end
