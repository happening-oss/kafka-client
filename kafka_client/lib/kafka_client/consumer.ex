defmodule KafkaClient.Consumer do
  use Parent.GenServer
  require Logger
  alias KafkaClient.Consumer.Port

  def start_link(opts), do: Parent.GenServer.start_link(__MODULE__, opts)

  @impl GenServer
  def init(opts) do
    handler = Keyword.fetch!(opts, :handler)
    port = Port.open(opts)
    {:ok, %{handler: handler, port: port, end_offsets: nil}}
  end

  @impl GenServer
  def handle_info({port, {:data, data}}, %{port: port} = state),
    do: handle_port_message(:erlang.binary_to_term(data), state)

  def handle_info({:caught_up, partition}, state) do
    state = update_in(state.end_offsets, &MapSet.delete(&1, partition))
    {:noreply, maybe_notify_caught_up(state)}
  end

  def handle_info({port, {:exit_status, status}}, %{port: port} = state) do
    Logger.error("port exited with status #{status}")
    {:stop, :port_crash, %{state | port: nil}}
  end

  @impl GenServer
  def terminate(_reason, state),
    do: if(state.port != nil, do: Port.close(state.port))

  @impl Parent.GenServer
  def handle_stopped_children(children, state) do
    if Enum.any?(Map.keys(children), &match?({:processor, {_topic, _partition}}, &1)),
      do: {:stop, :processor_crashed, state},
      else: {:noreply, state}
  end

  defp handle_port_message({:assigned, partitions} = event, state) do
    start_processors(state, partitions)
    state.handler.(event)
    {:noreply, state}
  end

  defp handle_port_message({:unassigned, partitions} = event, state) do
    Enum.each(partitions, &Parent.shutdown_child({:processor, &1}))
    state.handler.(event)
    {:noreply, state}
  end

  defp handle_port_message({:end_offsets, end_offsets}, state) do
    start_processors(state, end_offsets)

    end_offsets =
      for {topic, partition, offset} <- end_offsets,
          offset > 0,
          into: MapSet.new(),
          do: {topic, partition}

    {:noreply, maybe_notify_caught_up(%{state | end_offsets: end_offsets})}
  end

  defp handle_port_message(
         {:record, topic, partition, offset, timestamp, payload},
         state
       ) do
    now = System.monotonic_time()

    :telemetry.execute(
      [:kafka_client, :consumer, :record, :queue, :start],
      %{system_time: System.system_time(), monotonic_time: now},
      %{topic: topic, partition: partition, offset: offset, timestamp: timestamp}
    )

    {:ok, pid} = Parent.child_pid({:processor, {topic, partition}})
    KafkaClient.Consumer.Processor.handle_record(pid, offset, timestamp, payload, now)

    {:noreply, state}
  end

  defp handle_port_message({:metrics, transfer_time, duration}, state) do
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

  defp handle_port_message({:committed, _offsets} = event, state) do
    state.handler.(event)
    {:noreply, state}
  end

  defp maybe_notify_caught_up(state) do
    if MapSet.size(state.end_offsets) == 0 do
      state.handler.(:caught_up)
      %{state | end_offsets: nil}
    else
      state
    end
  end

  defp start_processors(state, partitions) do
    Enum.each(
      partitions,
      fn partition ->
        {topic, partition, end_offset} =
          with {topic, partition} <- partition, do: {topic, partition, nil}

        # if end_offset is 0, the topic is empty, so we'll send `nil` to the processor, to indicate
        # that it doesn't need to emit the caught_up notification
        end_offset = with 0 <- end_offset, do: nil

        {:ok, _pid} =
          Parent.start_child(
            {KafkaClient.Consumer.Processor,
             {self(), topic, partition, end_offset, state.handler, state.port}},
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
