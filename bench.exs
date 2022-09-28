bench_pid = self()

brokers = [{"localhost", 9092}]

topic = "kafka_client_bench"
num_partitions = 10
num_messages = 1_000_000
message_size = 5_000
message = String.duplicate("a", message_size)

metrics = :atomics.new(4, signed: false)
transfers = :ets.new(:transfers, [:public, write_concurrency: true])

:telemetry.attach(
  :elixir_bench,
  [:kafka_client, :consumer, :record, :queue, :stop],
  fn _name, measurements, _meta, _config ->
    count = :atomics.add_get(metrics, 1, 1)
    :atomics.add(metrics, 2, measurements.duration)
    if count == num_messages, do: send(bench_pid, :done)
  end,
  nil
)

:telemetry.attach(
  :java_bench,
  [:kafka_client, :consumer, :port, :stop],
  fn _name, measurements, _meta, _config ->
    :atomics.add(metrics, 3, 1)
    :atomics.add(metrics, 4, measurements.duration)
    :ets.insert(transfers, {make_ref(), measurements.transfer_time})
  end,
  nil
)

IO.puts("recreating topic #{topic}")
KafkaClient.TestAdmin.recreate_topic([{"localhost", 9092}], topic, num_partitions: num_partitions)

IO.puts("producing messages")

KafkaClient.Producer.start_link(servers: ["localhost:9092"], name: :bench_producer)

produced_count = :counters.new(1, [])

{produce_time, _} =
  :timer.tc(fn ->
    1..num_messages
    |> Enum.each(
      &KafkaClient.Producer.send(
        :bench_producer,
        %{topic: topic, partition: rem(&1, num_partitions), key: "key", value: message},
        on_completion: fn {:ok, _partition, _offset, _timestamp} ->
          :counters.add(produced_count, 1, 1)
          if :counters.get(produced_count, 1) == num_messages, do: send(bench_pid, :producer_done)
        end
      )
    )

    receive do
      :producer_done -> :ok
    end
  end)

IO.puts("produce throughput: #{round(num_messages * 1_000_000 / produce_time)} messages/s")

{:ok, consumer_pid} =
  KafkaClient.Consumer.start_link(
    servers: Enum.map(brokers, fn {host, port} -> "#{host}:#{port}" end),
    group_id: "test_group",
    subscriptions: [topic],
    handler: fn
      {:assigned, _partitions} -> send(bench_pid, :consuming)
      {:record, _record} -> :ok
    end
  )

receive do
  :consuming -> IO.puts("started consuming")
after
  :timer.seconds(10) -> raise "timeout"
end

{time, _} =
  :timer.tc(fn ->
    receive do
      :done -> :ok
    after
      :timer.minutes(2) -> raise "timeout"
    end
  end)

avg_elixir_queue_time =
  :atomics.get(metrics, 2)
  |> div(:atomics.get(metrics, 1))
  |> System.convert_time_unit(:native, :microsecond)

avg_java_queue_time =
  :atomics.get(metrics, 4)
  |> div(:atomics.get(metrics, 3))
  |> System.convert_time_unit(:native, :microsecond)

transfer_stats =
  transfers
  |> :ets.tab2list()
  |> Enum.map(fn {_, duration} -> duration end)
  |> Statistex.statistics(percentiles: [50, 90, 99])

transfer_times =
  [
    transfer_stats.average,
    transfer_stats.percentiles[90],
    transfer_stats.percentiles[99]
  ]
  |> Enum.map(&(&1 |> ceil() |> System.convert_time_unit(:native, :microsecond)))
  |> Enum.zip(~w/avg 90p 99p/)
  |> Enum.map(fn {value, label} -> "#{label}=#{value}us" end)
  |> Enum.join(" ")

IO.puts("""

message count: #{num_messages}
message size: #{message_size} bytes
concurrency (partitions count): #{num_partitions}

throughput: #{floor(num_messages / time * 1_000_000)} messages/s
average time in queue: #{max(avg_elixir_queue_time + avg_java_queue_time, 0)} us
java -> elixir transfer time: #{transfer_times}

""")

KafkaClient.Consumer.stop(consumer_pid)
