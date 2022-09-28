defmodule KafkaClient.ProducerTest do
  use ExUnit.Case, async: true

  import KafkaClient.Test.Helper
  alias KafkaClient.Producer

  test "produce" do
    [topic] = start_consumer!().subscriptions
    produced = produce(topic)

    consumed = assert_processing(topic, produced.partition)
    keys = ~w/key value timestamp headers/a
    assert Map.take(produced, keys) == Map.take(consumed, keys)
  end

  test "sync_produce" do
    [topic] = start_consumer!().subscriptions

    assert {:ok, produced} = sync_produce(topic, partition: nil, timestamp: nil)

    consumed = assert_processing(topic, produced.partition)
    keys = ~w/key value timestamp headers offset/a
    assert Map.take(produced, keys) == Map.take(consumed, keys)
  end

  test "stop" do
    {:ok, producer} = Producer.start_link(servers: servers())
    mref = Process.monitor(producer)
    Producer.stop(producer)
    assert_receive {:DOWN, ^mref, :process, ^producer, _reason}
  end
end
