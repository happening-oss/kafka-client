defmodule KafkaClient.ProducerTest do
  use ExUnit.Case, async: true

  import KafkaClient.Test.Helper, except: [produce: 1, produce: 2]
  alias KafkaClient.Producer

  setup do
    producer =
      start_supervised!(
        {Producer, servers: servers()},
        id: make_ref(),
        restart: :temporary
      )

    {:ok, producer: producer}
  end

  test "producing", ctx do
    topic = new_test_topic()
    recreate_topics([topic])

    produced = produce(ctx.producer, topic)

    start_consumer!(group_id: nil, subscriptions: [topic], recreate_topics?: false)
    consumed = assert_processing(topic, produced.partition)

    keys = ~w/key value timestamp/a
    assert Map.take(produced, keys) == Map.take(consumed, keys)
  end

  test "stop", ctx do
    mref = Process.monitor(ctx.producer)
    Producer.stop(ctx.producer)
    assert_receive {:DOWN, ^mref, :process, _pid, _reason}
  end

  defp produce(producer, topic, opts \\ []) do
    headers = for _ <- 1..5, do: {unique("header_key"), :crypto.strong_rand_bytes(4)}
    key = Keyword.get(opts, :key, :crypto.strong_rand_bytes(4))
    value = Keyword.get(opts, :value, :crypto.strong_rand_bytes(4))

    timestamp =
      DateTime.to_unix(~U[2022-01-01 00:00:00.00Z], :nanosecond) +
        System.unique_integer([:positive, :monotonic])

    partition = Keyword.get(opts, :partition, 0)

    record = %{
      topic: topic,
      key: key,
      value: value,
      timestamp: timestamp,
      partition: partition,
      headers: headers
    }

    Producer.send(producer, record)

    record
  end
end
