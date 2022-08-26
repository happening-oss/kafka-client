defmodule KafkaClient.ConsumerTest do
  use ExUnit.Case, async: true

  import KafkaClient.Consumer.Driver.Test, only: [produce: 1, produce: 2]

  test "produced message is consumed" do
    consumer = start_consumer!()
    produced_record = produce(consumer)

    assert_receive {:processing, processing_record}
    assert processing_record.topic == produced_record.topic
    assert processing_record.partition == produced_record.partition
    assert processing_record.offset == produced_record.offset

    assert resume_processing(processing_record) == :ok
  end

  test "messages with the same topic and partition are processed sequentially" do
    consumer = start_consumer!()

    %{topic: topic, partition: partition, offset: offset1} = produce(consumer)
    offset2 = produce(consumer, topic: topic, partition: partition).offset

    assert_receive {:processing, %{offset: ^offset1} = record1}
    refute_receive {:processing, _record2}

    resume_processing(record1)
    assert_receive {:processing, %{offset: ^offset2} = record2}
    assert resume_processing(record2) == :ok
  end

  test "messages with different topics and partitions are processed concurrently" do
    consumer = start_consumer!()

    %{topic: topic1, partition: partition1} = produce(consumer)
    produce(consumer, topic: topic1, partition: partition1 + 1)
    produce(consumer, topic: "#{topic1}_1", partition: partition1)
    produce(consumer, topic: "#{topic1}_2", partition: partition1 + 1)

    assert_receive {:processing, _}
    assert_receive {:processing, _}
    assert_receive {:processing, _}
    assert_receive {:processing, _}
  end

  defp start_consumer! do
    test_pid = self()

    start_supervised!(
      {KafkaClient.Consumer,
       servers: ["localhost:9092"],
       group_id: "mygroup",
       topics: ["mytopic"],
       handler: fn {:record, record} ->
         send(test_pid, {:processing, Map.put(record, :pid, self())})

         receive do
           :consume -> :ok
         end
       end,
       driver: KafkaClient.Consumer.Driver.Test}
    )
  end

  defp resume_processing(record) do
    mref = Process.monitor(record.pid)
    send(record.pid, :consume)
    assert_receive {:DOWN, ^mref, :process, _pid, exit_reason}
    if exit_reason == :normal, do: :ok, else: {:error, exit_reason}
  end
end
