defmodule KafkaClient.Consumer do
  use Parent.GenServer
  require Logger

  def start_link(opts) do
    servers = Keyword.fetch!(opts, :servers)
    topics = Keyword.fetch!(opts, :topics)
    group_id = Keyword.fetch!(opts, :group_id)
    handler = Keyword.fetch!(opts, :handler)

    driver_callback = Keyword.get(opts, :driver, KafkaClient.Consumer.Driver.Port)

    consumer_params = %{
      "bootstrap.servers" => Enum.join(servers, ","),
      "group.id" => group_id,
      "key.deserializer" => "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" => "org.apache.kafka.common.serialization.ByteArrayDeserializer",
      "max.poll.interval.ms" => 1000,
      "auto.offset.reset" => "earliest"
    }

    poll_duration = 10

    Parent.GenServer.start_link(
      __MODULE__,
      {consumer_params, topics, poll_duration, handler, driver_callback}
    )
  end

  @impl GenServer
  def init({consumer_params, topics, poll_duration, handler, driver_callback}) do
    state = %{
      consumer_params: consumer_params,
      topics: topics,
      poll_duration: poll_duration,
      handler: handler,
      driver_callback: driver_callback,
      driver_instance: driver_callback.open(consumer_params, topics, poll_duration),
      buffers: %{}
    }

    {:ok, state}
  end

  @impl GenServer
  def handle_info(
        {driver_instance, {:data, data}},
        %{driver_instance: driver_instance} = state
      ) do
    case :erlang.binary_to_term(data) do
      :consuming ->
        state.handler.(:consuming)
        {:noreply, state}

      {:record, topic, partition, offset, timestamp, payload} ->
        state =
          if Parent.child?({:processor, {topic, partition}}) do
            buffer =
              state.buffers
              |> Map.get_lazy({topic, partition}, &:queue.new/0)
              |> then(&:queue.in({offset, timestamp, payload}, &1))

            %{state | buffers: Map.put(state.buffers, {topic, partition}, buffer)}
          else
            start_processor!(topic, partition, offset, timestamp, payload, state.handler)
            state
          end

        {:noreply, state}
    end
  end

  def handle_info(
        {driver_instance, {:exit_status, status}},
        %{driver_instance: driver_instance} = state
      ) do
    Logger.error("consumer driver exited with status #{status}")

    driver_instance =
      state.driver_callback.open(state.consumer_params, state.topics, state.poll_interval)

    state = %{state | driver_instance: driver_instance}

    {:noreply, state}
  end

  @impl GenServer
  def terminate(_reason, state) do
    state.driver_callback.close(state.driver_instance)
  end

  @impl Parent.GenServer
  def handle_stopped_children(children, state) do
    state = Enum.reduce(children, state, &handle_stopped_child/2)
    {:noreply, state}
  end

  defp handle_stopped_child({{:processor, {topic, partition}}, _process_info}, state) do
    state.driver_callback.notify_processed(state.driver_instance, topic, partition)

    case Map.fetch(state.buffers, {topic, partition}) do
      :error ->
        state

      {:ok, buffer} ->
        case :queue.out(buffer) do
          {{:value, {offset, timestamp, payload}}, buffer} ->
            start_processor!(topic, partition, offset, timestamp, payload, state.handler)
            %{state | buffers: Map.put(state.buffers, {topic, partition}, buffer)}

          {:empty, _empty_buffer} ->
            %{state | buffers: Map.delete(state.buffers, {topic, partition})}
        end
    end
  end

  defp handle_stopped_child(_other_child, state), do: state

  defp start_processor!(topic, partition, offset, timestamp, payload, handler) do
    {:ok, pid} =
      Parent.start_child(%{
        id: {:processor, {topic, partition}},
        meta: {offset, timestamp},
        restart: :temporary,
        ephemeral?: true,
        start: fn ->
          Task.start_link(fn ->
            record = %{
              topic: topic,
              partition: partition,
              offset: offset,
              timestamp: timestamp,
              payload: payload
            }

            handler.({:record, record})
          end)
        end
      })

    pid
  end
end
