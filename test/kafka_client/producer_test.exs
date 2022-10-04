defmodule KafkaClient.ProducerTest do
  use ExUnit.Case, async: true

  import KafkaClient.Test.Helper
  alias KafkaClient.Producer

  @tag :require_kafka
  test "produce" do
    [topic] = start_consumer!().subscriptions
    produced = produce(topic)

    consumed = assert_processing(topic, produced.partition)
    keys = ~w/key value timestamp headers/a
    assert Map.take(produced, keys) == Map.take(consumed, keys)
  end

  @tag :require_kafka
  test "sync_send" do
    [topic] = start_consumer!().subscriptions

    produced = [
      %{topic: topic, key: "key 1", value: "value 1"},
      %{topic: topic, key: "key 2", value: "value 2"}
    ]

    assert [ok: record1, ok: record2] =
             Producer.sync_send(:test_producer, produced) |> Enum.to_list()

    keys = ~w/topic key value/a
    expected = produced |> Enum.map(&Map.take(&1, keys)) |> Enum.sort()
    consumed = [record1, record2] |> Enum.map(&Map.take(&1, keys)) |> Enum.sort()
    assert consumed == expected
  end

  @tag :require_kafka
  test "stop" do
    {:ok, producer} = Producer.start_link(servers: servers())
    mref = Process.monitor(producer)
    Producer.stop(producer)
    assert_receive {:DOWN, ^mref, :process, ^producer, _reason}
  end
end
