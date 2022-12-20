defmodule KafkaClient.ProducerTest do
  use ExUnit.Case, async: true
  import KafkaClient.Test.Helper
  alias KafkaClient.Producer

  @moduletag :require_kafka

  test "produce" do
    [topic] = start_consumer!().subscriptions
    produced = produce(topic)

    consumed = assert_processing(topic, produced.partition)
    keys = ~w/key value timestamp headers/a
    assert Map.take(produced, keys) == Map.take(consumed, keys)
  end

  test "produce - buffer size exceeded" do
    [topic] = start_consumer!().subscriptions

    {:ok, producer} =
      Producer.start_link(
        servers: servers(),
        producer_params: %{
          "internal.buffer_size" => 1024 * 1024
        }
      )

    produce_large_msg = fn key, topic ->
      Producer.send(
        producer,
        %{topic: topic, key: key, value: :crypto.strong_rand_bytes(1_000_000), partition: 0},
        on_completion: fn
          _ -> :ok
        end
      )
    end

    produced_response_1 = produce_large_msg.("key1", topic)
    produced_response_2 = produce_large_msg.("key2", topic)

    assert {produced_response_1, produced_response_2} == {:ok, {:error, :buffer_size_exceeded}}

    consumed = assert_processing(topic, 0)
    assert consumed.key == "key1"

    refute_processing(topic, 0)
  end

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

  test "stop" do
    {:ok, producer} = Producer.start_link(servers: servers())
    mref = Process.monitor(producer)
    Producer.stop(producer)
    assert_receive {:DOWN, ^mref, :process, ^producer, _reason}
  end

  test "metrics" do
    {:ok, producer} = Producer.start_link(servers: servers())
    {:ok, metrics} = Producer.metrics(producer)
    :ok = Producer.stop(producer)

    assert is_map(metrics)
  end
end
