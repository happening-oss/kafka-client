defmodule KafkaClient.Consumer do
  use Parent.GenServer
  require Logger
  alias KafkaClient.Consumer.Poller

  def start_link(opts), do: Parent.GenServer.start_link(__MODULE__, opts)

  @impl GenServer
  def init(opts) do
    {handler, opts} = Keyword.pop!(opts, :handler)

    {:ok, poller} =
      Parent.start_child(
        {Poller, Keyword.put(opts, :subscriber, self())},
        id: :poller,
        restart: :temporary,
        ephemeral?: true
      )

    {:ok, %{handler: handler, poller: poller}}
  end

  @impl GenServer
  def handle_info({poller, message}, %{poller: poller} = state) do
    handle_poller_message(message, state)
    {:noreply, state}
  end

  @impl Parent.GenServer
  def handle_stopped_children(children, state) do
    crashed_children = Map.keys(children)
    {:stop, {:children_crashed, crashed_children}, state}
  end

  defp handle_poller_message({:assigned, partitions} = event, state) do
    start_processors(state.handler, partitions)
    state.handler.(event)
  end

  defp handle_poller_message({:unassigned, partitions} = event, state) do
    Enum.each(partitions, &Parent.shutdown_child({:processor, &1}))
    state.handler.(event)
  end

  defp handle_poller_message({:record, record}, _state) do
    {:ok, pid} = Parent.child_pid({:processor, {record.topic, record.partition}})
    KafkaClient.Consumer.Processor.handle_record(pid, record)
  end

  defp handle_poller_message(message, state),
    do: state.handler.(message)

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
