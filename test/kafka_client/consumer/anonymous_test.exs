defmodule KafkaClient.Consumer.AnonymousTest do
  use ExUnit.Case, async: true
  import KafkaClient.Test.Helper

  @moduletag :require_kafka

  test "multiple consumers" do
    subscriptions = start_consumer!(group_id: nil, num_topics: 2, max_batch_size: 1).subscriptions

    start_consumer!(
      group_id: nil,
      subscriptions: subscriptions,
      recreate_topics?: false,
      max_batch_size: 1
    )

    [topic1, topic2] = subscriptions

    produced1 = sync_produce!(topic1, partition: 0)
    produced2 = sync_produce!(topic1, partition: 1)
    produced3 = sync_produce!(topic2, partition: 0)

    assert hd(assert_processing(topic1, 0).records).offset == produced1.offset
    assert hd(assert_processing(topic1, 0).records).offset == produced1.offset

    assert hd(assert_processing(topic1, 1).records).offset == produced2.offset
    assert hd(assert_processing(topic1, 1).records).offset == produced2.offset

    assert hd(assert_processing(topic2, 0).records).offset == produced3.offset
    assert hd(assert_processing(topic2, 0).records).offset == produced3.offset

    refute_processing(topic1, 0)
    refute_processing(topic1, 1)
    refute_processing(topic2, 0)

    start_consumer!(group_id: nil, subscriptions: subscriptions, recreate_topics?: false)

    assert hd(assert_processing(topic1, 0).records).offset == produced1.offset
    assert hd(assert_processing(topic1, 1).records).offset == produced2.offset
    assert hd(assert_processing(topic2, 0).records).offset == produced3.offset
  end

  test "caught up event" do
    # if the topics are empty, consumer should immediately get a caught-up notification
    consumer1 = start_consumer!(group_id: nil, num_topics: 2, max_batch_size: 1)
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
    Enum.each(produced, &process_next_batch!(&1.topic, &1.partition))
    refute_caught_up()

    # stop consumer and flush all notifications sent by it
    stop_consumer(consumer1)
    flush_messages()

    # start another consumer on the same topics
    start_consumer!(
      group_id: nil,
      subscriptions: consumer1.subscriptions,
      recreate_topics?: false,
      max_batch_size: 1
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
    Enum.each(almost_all_records, &process_next_batch!(&1.topic, &1.partition))
    refute_caught_up()

    # caught-up notification should be sent after the last previously produced record is processed
    process_next_batch!(final_record.topic, final_record.partition)
    assert_caught_up()

    # subsequent processing shouldn't trigger new caught-up notifications
    Enum.each(produced_after_connect, &process_next_batch!(&1.topic, &1.partition))
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

    [first_processing | _] = assert_processing(topic, 0).records
    assert first_processing.offset == record2.offset
  end

  test "initial position via offset for all partitions" do
    topic = new_test_topic()

    recreate_topics([topic])

    sync_produce!(topic, partition: 0)
    sync_produce!(topic, partition: 0)
    sync_produce!(topic, partition: 0)

    sync_produce!(topic, partition: 1)
    sync_produce!(topic, partition: 1)
    sync_produce!(topic, partition: 1)

    start_consumer!(
      group_id: nil,
      subscriptions: [{topic, :all, {:offset, 1}}],
      recreate_topics?: false
    )

    processing = assert_processing(topic, 0)
    assert processing.offset == 1

    processing2 = assert_processing(topic, 1)
    assert processing2.offset == 1
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

    [first_processing | _] = assert_processing(topic, 0).records
    assert first_processing.offset == record2.offset
  end

  test "initial position via timestamp for all paritions" do
    topic = new_test_topic()

    recreate_topics([topic])

    sync_produce!(topic, partition: 0)
    sync_produce!(topic, partition: 0)
    sync_produce!(topic, partition: 1)
    latest_record = sync_produce!(topic, partition: 1)

    # with +1, we will skip all records produced so far
    timestamp = latest_record.timestamp + 1

    record1 = sync_produce!(topic, partition: 0)
    record2 = sync_produce!(topic, partition: 1)

    start_consumer!(
      group_id: nil,
      subscriptions: [{topic, :all, {:timestamp, timestamp}}],
      recreate_topics?: false
    )

    processing = assert_processing(topic, 0)
    assert processing.offset == record1.offset

    processing2 = assert_processing(topic, 1)
    assert processing2.offset == record2.offset
  end

  test "initial position via timestamp for all paritions mixed" do
    topic1 = new_test_topic()
    topic2 = new_test_topic()

    recreate_topics([topic1, topic2])

    sync_produce!(topic1, partition: 0)
    sync_produce!(topic1, partition: 0)
    sync_produce!(topic1, partition: 1)
    latest_record = sync_produce!(topic1, partition: 1)

    # with +1, we will skip all records produced so far
    timestamp = latest_record.timestamp + 1

    record1 = sync_produce!(topic1, partition: 0)
    record2 = sync_produce!(topic1, partition: 1)

    sync_produce!(topic2, partition: 0)
    sync_produce!(topic2, partition: 0)
    sync_produce!(topic2, partition: 1)
    sync_produce!(topic2, partition: 1)

    start_consumer!(
      group_id: nil,
      subscriptions: [
        {topic1, :all, {:timestamp, timestamp}},
        topic2
      ],
      recreate_topics?: false
    )

    processing_topic1_part_0 = assert_processing(topic1, 0)
    assert processing_topic1_part_0.offset == record1.offset

    processing_topic1_part1 = assert_processing(topic1, 1)
    assert processing_topic1_part1.offset == record2.offset

    processing_topic2_part0 = assert_processing(topic2, 0)
    assert processing_topic2_part0.offset == 0

    processing_topic2_part1 = assert_processing(topic2, 1)
    assert processing_topic2_part1.offset == 0
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
