defmodule KafkaClient.Consumer.Port do
  def open(opts) do
    servers = Keyword.fetch!(opts, :servers)
    topics = Keyword.fetch!(opts, :topics)
    group_id = Keyword.get(opts, :group_id)
    user_consumer_params = Keyword.get(opts, :consumer_params, %{})

    poller_properties = %{
      "poll_duration" => Keyword.get(opts, :poll_duration, 10),
      "commit_interval" => Keyword.get(opts, :commit_interval, :timer.seconds(5))
    }

    consumer_params =
      %{
        "heartbeat.interval.ms" => 100,
        "max.poll.interval.ms" => 1000,
        "auto.offset.reset" => "earliest"
      }
      |> Map.merge(user_consumer_params)
      # non-overridable params
      |> Map.merge(%{
        "bootstrap.servers" => Enum.join(servers, ","),
        "group.id" => group_id,
        "enable.auto.commit" => false,
        "key.deserializer" => "org.apache.kafka.common.serialization.StringDeserializer",
        "value.deserializer" => "org.apache.kafka.common.serialization.ByteArrayDeserializer"
      })

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
          "com.superology.kafka.ConsumerPort",
          consumer_params |> :erlang.term_to_binary() |> Base.encode64(),
          topics |> :erlang.term_to_binary() |> Base.encode64(),
          poller_properties |> :erlang.term_to_binary() |> Base.encode64()
        ]
      ]
    )
  end

  def close(port) do
    Port.command(port, :erlang.term_to_binary({:stop}))

    receive do
      {^port, {:exit_status, 0}} -> :ok
      {^port, {:exit_status, status}} -> {:error, status}
    after
      :timer.seconds(5) -> {:error, :timeout}
    end
  end

  def ack(port, topic, partition, offset) do
    Port.command(
      port,
      :erlang.term_to_binary({:ack, topic, partition, offset})
    )
  end
end
