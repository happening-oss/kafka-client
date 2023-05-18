defmodule KafkaClient.Consumer.ProcessingTest do
  use ExUnit.Case, async: true
  import KafkaClient.Test.Helper
  alias KafkaClient.Admin

  @moduletag :require_kafka

  test "sequential processing on a single topic-partition" do
    consumer = start_consumer!(max_batch_size: 1)
    [topic] = consumer.subscriptions

    produced1 = sync_produce!(topic, partition: 0)
    produced2 = sync_produce!(topic, partition: 0)

    batch1 = assert_processing(topic, 0)
    assert [record1] = batch1.records
    assert record1.offset == produced1.offset
    assert record1.headers == produced1.headers
    assert record1.key == produced1.key
    assert record1.value == produced1.value

    refute_processing(topic, 0)

    resume_processing(batch1)

    batch2 = assert_processing(topic, 0)
    assert [record2] = batch2.records
    assert record2.offset == produced2.offset
    assert record2.headers == produced2.headers
    assert record2.key == produced2.key
    assert record2.value == produced2.value
  end

  test "batch size" do
    consumer = start_consumer!(max_batch_size: 3)
    [topic] = consumer.subscriptions

    sync_produce!(topic, partition: 0)

    # as soon as something is produced, the first batch of size 1 starts processing
    batch1 = assert_processing(topic, 0)
    assert length(batch1.records) == 1

    # produce some more records while the 1st batch is still processing
    produced_offsets = Enum.map(1..8, fn _ -> sync_produce!(topic, partition: 0).offset end)
    Enum.each(produced_offsets, &assert_polled(topic, 0, &1))

    # finish batch1
    resume_processing(batch1)

    # 8 records are produced and max_batch_size == 3 => we expect 3 batches of sizes [3, 3, 2]
    batches = Enum.map(1..3, fn _ -> process_next_batch!(topic, 0) end)
    assert Enum.map(batches, &length(&1.records)) == [3, 3, 2]

    # check that the messages are properly ordered
    consumed_offsets = for batch <- batches, record <- batch.records, do: record.offset
    assert consumed_offsets == produced_offsets
  end

  test "concurrent processing" do
    consumer = start_consumer!(num_topics: 2)
    [topic1, topic2] = consumer.subscriptions

    sync_produce!(topic1, partition: 0)
    sync_produce!(topic1, partition: 1)
    sync_produce!(topic2, partition: 0)

    assert_processing(topic1, 0)
    assert_processing(topic1, 1)
    assert_processing(topic2, 0)
  end

  test "processed messages are committed" do
    consumer = start_consumer!(max_batch_size: 1)
    [topic] = consumer.subscriptions

    sync_produce!(topic, partition: 0)
    sync_produce!(topic, partition: 0)
    sync_produce!(topic, partition: 0)

    sync_produce!(topic, partition: 1)
    sync_produce!(topic, partition: 1)
    sync_produce!(topic, partition: 1)
    sync_produce!(topic, partition: 1)
    sync_produce!(topic, partition: 1)

    [last_processed_record_partition_0] = process_next_batch!(topic, 0).records

    process_next_batch!(topic, 1)
    [last_processed_record_partition_1] = process_next_batch!(topic, 1).records

    admin = start_supervised!({Admin, servers: servers()})

    eventually(
      fn ->
        group_id = consumer.group_id
        partitions = [{topic, 0}, {topic, 1}]
        {:ok, committed} = Admin.list_consumer_group_offsets(admin, group_id, partitions)

        assert committed ==
                 %{
                   {topic, 0} => last_processed_record_partition_0.offset + 1,
                   {topic, 1} => last_processed_record_partition_1.offset + 1
                 }
      end,
      attempts: 20,
      delay: 500
    )
  end

  test "handler exception" do
    consumer = start_consumer!()
    [topic] = consumer.subscriptions

    sync_produce!(topic, partition: 0)
    produced2 = sync_produce!(topic, partition: 0)

    batch = assert_processing(topic, 0)

    ExUnit.CaptureLog.capture_log(fn ->
      crash_processing(batch, "some reason")

      batch = assert_processing(topic, 0)
      record2 = hd(batch.records)
      assert record2.offset == produced2.offset
      assert record2.value == produced2.value
    end)
  end
end
