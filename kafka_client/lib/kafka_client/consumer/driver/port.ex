defmodule KafkaClient.Consumer.Driver.Port do
  @behaviour KafkaClient.Consumer.Driver

  @impl KafkaClient.Consumer.Driver
  def open(consumer_params, topics, poll_interval) do
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

  @impl KafkaClient.Consumer.Driver
  def close(port) do
    mref = Port.monitor(port)
    Port.close(port)

    receive do
      {:DOWN, ^mref, _, _, _status} -> :ok
    end
  end

  @impl KafkaClient.Consumer.Driver
  def notify_processed(port, topic, partition) do
    Port.command(port, :erlang.term_to_binary({:notify_processed, topic, partition}))
    :ok
  end
end
