defmodule Consumer.StreamTest do
  use ExUnit.Case, async: true
  import KafkaClient.Test.Helper
  alias KafkaClient.Consumer

  test "consuming until the end" do
    topic1 = new_test_topic()
    topic2 = new_test_topic()

    recreate_topics([topic1, topic2])

    produced =
      [
        sync_produce!(topic1, partition: 0),
        sync_produce!(topic1, partition: 1),
        sync_produce!(topic1, partition: 1),
        sync_produce!(topic2, partition: 0),
        sync_produce!(topic2, partition: 0),
        sync_produce!(topic2, partition: 0)
      ]
      |> Enum.map(&Map.take(&1, ~w/topic partition offset value/a))
      |> Enum.sort()

    events =
      Consumer.Stream.new(servers: servers(), subscriptions: [topic1, topic2])
      |> Stream.each(fn message ->
        with {:record, record} <- message,
             do: Consumer.Poller.ack(record)
      end)
      |> Enum.take_while(&(&1 != :caught_up))

    # expected events are: assigned, produced records, and caught_up (which is already removed with
    # Enum.take_while)
    assert length(events) == length(produced) + 1

    assert {:assigned, _partitions} = hd(events)

    consumed =
      for({:record, record} <- events, do: Map.take(record, ~w/topic partition offset value/a))
      |> Enum.sort()

    assert consumed == produced
  end

  test "cleanup" do
    topic1 = new_test_topic()

    recreate_topics([topic1])

    sync_produce!(topic1, partition: 0)
    sync_produce!(topic1, partition: 0)
    sync_produce!(topic1, partition: 0)

    # process only the first element
    Consumer.Stream.new(servers: servers(), subscriptions: [topic1])
    |> Enum.take(1)

    # check that the mailbox is empty
    refute_receive _
  end
end
