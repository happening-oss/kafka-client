defmodule KafkaClient.Consumer do
  use Parent.GenServer
  require Logger

  def start_link(opts) do
    servers = Keyword.fetch!(opts, :servers)
    topics = Keyword.fetch!(opts, :topics)
    group_id = Keyword.fetch!(opts, :group_id)
    handler = Keyword.fetch!(opts, :handler)

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
      {consumer_params, topics, poll_duration, handler}
    )
  end

  @impl GenServer
  def init({consumer_params, topics, poll_duration, handler}) do
    state = %{
      consumer_params: consumer_params,
      topics: topics,
      poll_duration: poll_duration,
      handler: handler,
      port: open_port(consumer_params, topics, poll_duration),
      buffers: %{}
    }

    {:ok, state}
  end

  @impl GenServer
  def handle_info({port, {:data, data}}, %{port: port} = state) do
    case :erlang.binary_to_term(data) do
      :consuming ->
        state.handler.(:consuming)
        {:noreply, state}

      {:record, topic, partition, offset, timestamp, payload} ->
        state.handler.({:polled, topic, partition, offset, timestamp})

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

  def handle_info({port, {:exit_status, status}}, %{port: port} = state) do
    Logger.error("port exited with status #{status}")

    port = open_port(state.consumer_params, state.topics, state.poll_duration)
    state = %{state | port: port}

    {:noreply, state}
  end

  def handle_info({:EXIT, port, _reason}, state) when is_port(port),
    do: {:noreply, state}

  @impl Parent.GenServer
  def handle_stopped_children(children, state) do
    state = Enum.reduce(children, state, &handle_stopped_child/2)
    {:noreply, state}
  end

  defp handle_stopped_child({{:processor, {topic, partition}}, _process_info}, state) do
    Port.command(state.port, :erlang.term_to_binary({:notify_processed, topic, partition}))

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

  defp open_port(consumer_params, topics, poll_interval) do
    Port.open(
      {:spawn_executable, System.find_executable("java")},
      [
        :nouse_stdio,
        :binary,
        :exit_status,
        packet: 4,
        args: [
          "-cp",
          "#{Application.app_dir(:kafka_client)}/priv/kafka-client-1.0.jar",
          "com.superology.KafkaConsumerPort",
          consumer_params |> :erlang.term_to_binary() |> Base.encode64(),
          topics |> :erlang.term_to_binary() |> Base.encode64(),
          poll_interval |> :erlang.term_to_binary() |> Base.encode64()
        ]
      ]
    )
  end
end
