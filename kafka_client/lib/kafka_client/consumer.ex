defmodule KafkaClient.Consumer do
  def start do
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
            "com.superology.KafkaConsumerPort"
          ]
        ]
      )

    Stream.repeatedly(fn ->
      Port.command(port, :erlang.term_to_binary({:poll, 100}))

      receive do
        {^port, {:data, data}} -> IO.inspect(:erlang.binary_to_term(data))
      after
        100 -> :ok
      end
    end)
    |> Stream.run()
  end
end
