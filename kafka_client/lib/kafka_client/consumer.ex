defmodule KafkaClient.Consumer do
  use Parent.GenServer
  require Logger
  alias KafkaClient.Consumer.Poller

  def start_link(opts), do: Parent.GenServer.start_link(__MODULE__, opts)

  @impl GenServer
  def init(opts) do
    {handler, opts} = Keyword.pop!(opts, :handler)

    {:ok, _subscriber} =
      Parent.start_child(
        {Poller, Keyword.put(opts, :subscriber, self())},
        id: :poller,
        restart: :temporary,
        ephemeral?: true
      )

    {:ok, handler}
  end

  @impl GenServer
  def handle_info(:caught_up, handler) do
    handler.(:caught_up)
    {:noreply, handler}
  end

  def handle_info({:assigned, partitions} = event, handler) do
    start_processors(handler, partitions)
    handler.(event)
    {:noreply, handler}
  end

  def handle_info({:unassigned, partitions} = event, handler) do
    Enum.each(partitions, &Parent.shutdown_child({:processor, &1}))
    handler.(event)
    {:noreply, handler}
  end

  def handle_info({:record, record}, handler) do
    {:ok, pid} = Parent.child_pid({:processor, {record.topic, record.partition}})
    KafkaClient.Consumer.Processor.handle_record(pid, record)
    {:noreply, handler}
  end

  def handle_info({:committed, _offsets} = event, handler) do
    handler.(event)
    {:noreply, handler}
  end

  @impl Parent.GenServer
  def handle_stopped_children(children, handler) do
    crashed_children = Map.keys(children)
    {:stop, {:children_crashed, crashed_children}, handler}
  end

  defp start_processors(handler, partitions) do
    Enum.each(
      partitions,
      fn {topic, partition} ->
        {:ok, _pid} =
          Parent.start_child(
            {KafkaClient.Consumer.Processor, handler},
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
