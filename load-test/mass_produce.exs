defmodule A do
  @kafka_value :crypto.strong_rand_bytes(1_000_000)

  def produce(num) do
    result = KafkaClient.Producer.send(
    :default_producer,
    %{topic: "bla", key: "#{num}_key", value: @kafka_value, partition: 0},
    on_completion: fn
      {:ok, _, offset, _} ->
        IO.write("O")
      _ ->
        IO.write("Z")
    end
    )

    case result do
      :ok ->
       IO.write(".")
      {:error, error} ->
        IO.write("x")
        :timer.sleep(10)
        A.produce(num)
    end
  end
end

KafkaClient.Producer.start_link(
      servers: ["kafka:19092"],
      name: :default_producer,
      producer_params: %{
        "max.block.ms" => 60_000,
        "buffer.memory" => 1*1024*1024,
        "internal.buffer_size" => 768*1024*1024
      }
    )

1..100_000
|> Enum.each(fn n ->
  A.produce(n)
end)
