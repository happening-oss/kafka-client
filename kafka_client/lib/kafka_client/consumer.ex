defmodule KafkaClient.Consumer do
  use GenServer

  def start_link do
    params = %{
      "bootstrap.servers" => "localhost:9092",
      "group.id" => "test",
      "key.deserializer" => "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" => "org.apache.kafka.common.serialization.ByteArrayDeserializer",
      "max.poll.interval.ms" => 1000
    }

    topics = ["mytopic"]

    poll_duration = 100
    GenServer.start_link(__MODULE__, {params, topics, poll_duration})
  end

  @impl GenServer
  def init({params, topics, poll_duration}) do
    port =
      Port.open(
        {:spawn_executable, System.find_executable("java")},
        [
          :nouse_stdio,
          :binary,
          packet: 4,
          args: [
            "-cp",
            "#{Application.app_dir(:kafka_client)}/priv/kafka-client-1.0.jar",
            "com.superology.KafkaConsumerPort",
            params |> :erlang.term_to_binary() |> Base.encode64(),
            topics |> :erlang.term_to_binary() |> Base.encode64()
          ]
        ]
      )

    state = %{port: port, poll_duration: poll_duration}
    poll(state)

    {:ok, state}
  end

  @impl GenServer
  def handle_info(:poll, state) do
    poll(state)
    {:noreply, state}
  end

  def handle_info({port, {:data, data}}, %{port: port} = state) do
    IO.inspect(:erlang.binary_to_term(data))
    {:noreply, state}
  end

  defp poll(state) do
    Port.command(state.port, :erlang.term_to_binary({:poll, state.poll_duration}))
    Process.send_after(self(), :poll, state.poll_duration)
  end
end
