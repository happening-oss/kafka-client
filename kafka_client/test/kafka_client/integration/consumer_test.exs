defmodule KafkaClient.Integration.ConsumerTest do
  use ExUnit.Case, async: true

  @moduletag :integration

  test "consumes kafka messages" do
    brokers = [{"localhost", 9092}]
    topic = "kafka_client_test_topic"

    KafkaClient.Admin.recreate_topic(brokers, topic)

    test_pid = self()

    start_supervised!(
      {KafkaClient.Consumer,
       servers: Enum.map(brokers, fn {host, port} -> "#{host}:#{port}" end),
       group_id: "test_group",
       topics: [topic],
       handler: &send(test_pid, &1)}
    )

    :ok = :brod.start_client(brokers, :test_client, auto_start_producers: true)

    {:ok, offset1} = :brod.produce_sync_offset(:test_client, topic, 0, "key", "foo")
    {:ok, offset2} = :brod.produce_sync_offset(:test_client, topic, 0, "key", "bar")

    assert_receive :consuming, :timer.seconds(10)

    assert_receive {:record, record1}, :timer.seconds(5)
    assert %{topic: ^topic, partition: 0, offset: ^offset1, payload: "foo"} = record1

    assert_receive {:record, record2}, :timer.seconds(5)
    assert %{topic: ^topic, partition: 0, offset: ^offset2, payload: "bar"} = record2
  end
end
