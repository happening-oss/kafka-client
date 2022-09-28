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

  test "produce", ctx do
    [topic] = start_consumer!().subscriptions
    produced = produce(ctx.producer, topic)

    consumed = assert_processing(topic, produced.partition)
    keys = ~w/key value timestamp headers/a
    assert Map.take(produced, keys) == Map.take(consumed, keys)
  end

  test "sync_produce", ctx do
    [topic] = start_consumer!().subscriptions

    assert {:ok, produced} = sync_produce(ctx.producer, topic, partition: nil, timestamp: nil)

    consumed = assert_processing(topic, produced.partition)
    keys = ~w/key value timestamp headers offset/a
    assert Map.take(produced, keys) == Map.take(consumed, keys)
  end

  test "stop", ctx do
    mref = Process.monitor(ctx.producer)
    Producer.stop(ctx.producer)
    assert_receive {:DOWN, ^mref, :process, _pid, _reason}
  end

  defp produce(producer, topic, opts \\ []) do
    record = record(topic, opts)
    Producer.send(producer, record)
    record
  end

  defp sync_produce(producer, topic, opts) do
    record = record(topic, opts)

    with {:ok, partition, offset} <- Producer.sync_send(producer, record, :infinity),
         do: {:ok, %{record | partition: partition} |> Map.put(:offset, offset)}
  end

  defp record(topic, opts) do
    headers = for _ <- 1..5, do: {unique("header_key"), :crypto.strong_rand_bytes(4)}
    key = Keyword.get(opts, :key, :crypto.strong_rand_bytes(4))
    value = Keyword.get(opts, :value, :crypto.strong_rand_bytes(4))

    timestamp =
      DateTime.to_unix(~U[2022-01-01 00:00:00.00Z], :nanosecond) +
        System.unique_integer([:positive, :monotonic])

    partition = Keyword.get(opts, :partition, 0)

    %{
      topic: topic,
      key: key,
      value: value,
      timestamp: timestamp,
      partition: partition,
      headers: headers
    }
  end
end
