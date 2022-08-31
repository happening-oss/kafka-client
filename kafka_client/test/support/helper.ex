defmodule KafkaClient.Test.Helper do
  import ExUnit.Assertions

  def start_consumer!(opts \\ []) do
    brokers = [{"localhost", 9092}]
    test_pid = self()

    topics =
      Enum.map(
        1..Keyword.get(opts, :num_topics, 1)//1,
        fn _ -> "kafka_client_test_topic_#{System.unique_integer([:positive, :monotonic])}" end
      )

    topics
    |> Task.async_stream(
      &KafkaClient.Admin.recreate_topic(brokers, &1, num_partitions: 2),
      timeout: :timer.seconds(10)
    )
    |> Stream.run()

    child_id = make_ref()

    pid =
      ExUnit.Callbacks.start_supervised!(
        {KafkaClient.Consumer,
         servers: Enum.map(brokers, fn {host, port} -> "#{host}:#{port}" end),
         group_id: "test_group",
         topics: topics,
         handler: fn
           :consuming ->
             send(test_pid, :consuming)

           {:record, record} ->
             send(test_pid, {:processing, Map.put(record, :pid, self())})

             receive do
               :consume -> :ok
             end
         end},
        id: child_id,
        restart: :temporary
      )

    assert_receive :consuming, :timer.seconds(10)

    :ok = :brod.start_client(brokers, :test_client, auto_start_producers: true)
    %{pid: pid, child_id: child_id, topics: topics}
  end

  def stop_consumer(consumer), do: ExUnit.Callbacks.stop_supervised(consumer.child_id)

  def produce(topic, opts \\ []) do
    default_opts = %{partition: 0, key: "key", payload: :crypto.strong_rand_bytes(4)}
    opts = Map.merge(default_opts, Map.new(opts))

    {:ok, offset} =
      :brod.produce_sync_offset(:test_client, topic, opts.partition, opts.key, opts.payload)

    Map.put(opts, :offset, offset)
  end

  def resume_processing(record) do
    mref = Process.monitor(record.pid)
    send(record.pid, :consume)
    assert_receive {:DOWN, ^mref, :process, _pid, exit_reason}
    if exit_reason == :normal, do: :ok, else: {:error, exit_reason}
  end

  def assert_started_processing(topic, partition) do
    assert_receive {:processing, %{topic: ^topic, partition: ^partition} = record}
    record
  end

  def refute_started_processing(topic, partition) do
    refute_receive {:processing, %{topic: ^topic, partition: ^partition}}
  end

  def buffers(consumer), do: state(consumer).buffers
  def port(consumer), do: state(consumer).port

  def os_pid(port) do
    {:os_pid, os_pid} = Port.info(port, :os_pid)
    os_pid
  end

  defp state(consumer), do: :sys.get_state(consumer.pid)
end
