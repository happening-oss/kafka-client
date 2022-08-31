brokers = [{"localhost", 9092}]

topic = "kafka_client_bench"
num_partitions = 10
num_messages = 1_000_000
message_size = 10_000
batch_size = div(1_000_000, message_size) |> max(1) |> min(100)
message = String.duplicate("a", message_size)

IO.puts("recreating topic #{topic}")
KafkaClient.Admin.recreate_topic([{"localhost", 9092}], topic, num_partitions: num_partitions)

IO.puts("producing messages")
:ok = :brod.start_client(brokers, :test_client, auto_start_producers: true)

1..num_messages
|> Enum.map(&rem(&1, num_partitions))
|> Enum.group_by(& &1, fn _ -> %{key: "key", value: message} end)
|> Task.async_stream(
  fn {partition, messages} ->
    messages
    |> Enum.chunk_every(batch_size)
    |> Enum.each(&(:ok = :brod.produce_sync(:test_client, topic, partition, "key", &1)))
  end,
  ordered: false,
  timeout: :infinity
)
|> Stream.run()

bench_pid = self()

KafkaClient.Consumer.start_link(
  servers: Enum.map(brokers, fn {host, port} -> "#{host}:#{port}" end),
  group_id: "test_group",
  topics: [topic],
  handler: fn
    :consuming -> send(bench_pid, :consuming)
    {:record, _record} -> send(bench_pid, :message_processing)
  end
)

receive do
  :consuming -> IO.puts("started consuming")
after
  :timer.seconds(10) -> raise "timeout"
end

{time, _} =
  :timer.tc(fn ->
    Enum.each(1..num_messages, fn _ ->
      receive do
        :message_processing -> :ok
      after
        :timer.seconds(5) -> raise "timeout"
      end
    end)
  end)

IO.puts("""

message count: #{num_messages}
message size: #{message_size} bytes
concurrency (partitions count): #{num_partitions}

total time: #{div(time, 1000)} ms
throughput: #{floor(num_messages / time * 1_000_000)} messages/sec

""")
