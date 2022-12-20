defmodule KafkaClient.Consumer.AnonymousTest do
  use ExUnit.Case, async: true
  import KafkaClient.Test.Helper

  @moduletag :require_kafka

  test "multiple consumers" do
    subscriptions = start_consumer!(group_id: nil, num_topics: 2).subscriptions
    start_consumer!(group_id: nil, subscriptions: subscriptions, recreate_topics?: false)

    [topic1, topic2] = subscriptions

    produced1 = sync_produce!(topic1, partition: 0)
    produced2 = sync_produce!(topic1, partition: 1)
    produced3 = sync_produce!(topic2, partition: 0)

    assert assert_processing(topic1, 0).offset == produced1.offset
    assert assert_processing(topic1, 0).offset == produced1.offset

    assert assert_processing(topic1, 1).offset == produced2.offset
    assert assert_processing(topic1, 1).offset == produced2.offset

    assert assert_processing(topic2, 0).offset == produced3.offset
    assert assert_processing(topic2, 0).offset == produced3.offset

    refute_processing(topic1, 0)
    refute_processing(topic1, 1)
    refute_processing(topic2, 0)

    start_consumer!(group_id: nil, subscriptions: subscriptions, recreate_topics?: false)

    assert assert_processing(topic1, 0).offset == produced1.offset
    assert assert_processing(topic1, 1).offset == produced2.offset
    assert assert_processing(topic2, 0).offset == produced3.offset
  end

  test "caught up event" do
    # if the topics are empty, consumer should immediately get a caught-up notification
    consumer1 = start_consumer!(group_id: nil, num_topics: 2)
    assert_caught_up()

    # produce some messages
    [topic1, topic2] = consumer1.subscriptions

    produced = [
      sync_produce!(topic1, partition: 0),
      sync_produce!(topic1, partition: 0),
      sync_produce!(topic1, partition: 0),

      #
      sync_produce!(topic1, partition: 1),
      sync_produce!(topic1, partition: 1),

      #
      sync_produce!(topic2, partition: 0)
    ]

    # check that caught-up is sent only once
    Enum.each(produced, &process_next_record!(&1.topic, &1.partition))
    refute_caught_up()

    # stop consumer and flush all notifications sent by it
    stop_consumer(consumer1)
    flush_messages()

    # start another consumer on the same topics
    start_consumer!(
      group_id: nil,
      subscriptions: consumer1.subscriptions,
      recreate_topics?: false
    )

    # wait until all the records are polled
    Enum.each(produced, &assert_polled(&1.topic, &1.partition, &1.offset))

    # since topics are not empty now, we shouldn't get a caught-up notification yet
    refute_caught_up()

    # produce more messages after the consumer is connected
    produced_after_connect = [
      sync_produce!(topic1, partition: 0),
      sync_produce!(topic1, partition: 1),
      sync_produce!(topic2, partition: 0)
    ]

    # wait until all the records are polled
    Enum.each(produced_after_connect, &assert_polled(&1.topic, &1.partition, &1.offset))

    # process almost all previously produced records
    {almost_all_records, [final_record]} = Enum.split(produced, -1)
    Enum.each(almost_all_records, &process_next_record!(&1.topic, &1.partition))
    refute_caught_up()

    # caught-up notification should be sent after the last previously produced record is processed
    process_next_record!(final_record.topic, final_record.partition)
    assert_caught_up()

    # subsequent processing shouldn't trigger new caught-up notifications
    Enum.each(produced_after_connect, &process_next_record!(&1.topic, &1.partition))
    refute_caught_up()
  end

  test "partition assignment" do
    topic1 = new_test_topic()
    topic2 = new_test_topic()
    topic3 = new_test_topic()

    recreate_topics([topic1, topic2, topic3])

    start_consumer!(group_id: nil, subscriptions: [topic1, {topic2, 0}], recreate_topics?: false)

    for topic <- [topic1, topic2, topic3],
        partition <- 0..1,
        do: sync_produce!(topic, partition: partition)

    assert_processing(topic1, 0)
    assert_processing(topic1, 1)
    assert_processing(topic2, 0)
    refute_processing(topic2, 1)
    refute_processing(topic3, 0)
    refute_processing(topic3, 1)
  end

  test "initial position via offset" do
    topic = new_test_topic()

    recreate_topics([topic])

    sync_produce!(topic, partition: 0)
    record2 = sync_produce!(topic, partition: 0)
    sync_produce!(topic, partition: 0)

    start_consumer!(
      group_id: nil,
      subscriptions: [{topic, 0, record2.offset}],
      recreate_topics?: false
    )

    processing = assert_processing(topic, 0)
    assert processing.offset == record2.offset
  end

  test "initial position via timestamp" do
    topic = new_test_topic()

    recreate_topics([topic])

    sync_produce!(topic, partition: 0)
    record2 = sync_produce!(topic, partition: 0)
    sync_produce!(topic, partition: 0)

    start_consumer!(
      group_id: nil,
      subscriptions: [{topic, 0, {:timestamp, record2.timestamp}}],
      recreate_topics?: false
    )

    processing = assert_processing(topic, 0)
    assert processing.offset == record2.offset
  end

  defp flush_messages do
    Stream.repeatedly(fn ->
      receive do
        x -> x
      after
        0 -> nil
      end
    end)
    |> Stream.take_while(&(&1 != nil))
    |> Stream.run()
  end
end
