defmodule KafkaClient.AdminTest do
  use ExUnit.Case, async: true

  import KafkaClient.Test.Helper
  alias KafkaClient.Admin

  setup do
    admin =
      start_supervised!(
        {Admin, servers: servers()},
        id: make_ref(),
        restart: :temporary
      )

    {:ok, admin: admin}
  end

  @tag :require_kafka
  test "list_topics", ctx do
    topic = new_test_topic()
    recreate_topics([topic])
    assert topic in Admin.list_topics(ctx.admin)
  end

  @tag :require_kafka
  test "describe_topics", ctx do
    assert {:error, error} = Admin.describe_topics(ctx.admin, ["unknown_topic"])
    assert error == "This server does not host this topic-partition."

    topic1 = new_test_topic()
    topic2 = new_test_topic()

    recreate_topics([{topic1, 1}, {topic2, 2}])

    assert {:ok, topics} = Admin.describe_topics(ctx.admin, [topic1, topic2])
    assert topics == %{topic1 => [0], topic2 => [0, 1]}
  end

  @tag :require_kafka
  test "list_end_offsets", ctx do
    assert {:error, error} = Admin.list_end_offsets(ctx.admin, [{"unknown_topic", 0}])
    assert error == "This server does not host this topic-partition."

    topic1 = new_test_topic()
    topic2 = new_test_topic()
    recreate_topics([topic1, topic2])

    offset_topic1_partition0 = sync_produce!(topic1, partition: 0).offset

    sync_produce!(topic1, partition: 1)
    offset_topic1_partition1 = sync_produce!(topic1, partition: 1).offset

    topic_partitions = [{topic1, 0}, {topic1, 1}, {topic2, 0}]
    assert {:ok, mapping} = Admin.list_end_offsets(ctx.admin, topic_partitions)

    assert mapping == %{
             {topic1, 0} => offset_topic1_partition0 + 1,
             {topic1, 1} => offset_topic1_partition1 + 1,
             {topic2, 0} => 0
           }
  end

  @tag :require_kafka
  test "list_earliest_offsets", ctx do
    assert {:error, error} = Admin.list_end_offsets(ctx.admin, [{"unknown_topic", 0}])
    assert error == "This server does not host this topic-partition."

    topic1 = new_test_topic()
    topic2 = new_test_topic()
    recreate_topics([topic1, topic2])

    topic_partitions = [{topic1, 0}, {topic1, 1}, {topic2, 0}]
    assert {:ok, mapping} = Admin.list_end_offsets(ctx.admin, topic_partitions)

    assert mapping == %{
             {topic1, 0} => 0,
             {topic1, 1} => 0,
             {topic2, 0} => 0
           }
  end

  @tag :require_kafka
  test "list_consumer_group_offsets", ctx do
    consumer = start_consumer!()
    [topic] = consumer.subscriptions

    sync_produce!(topic, partition: 0)
    sync_produce!(topic, partition: 0)
    sync_produce!(topic, partition: 0)
    last_processed_offset_partition_0 = process_next_record!(topic, 0).offset

    stop_supervised!(consumer.child_id)

    group_id = consumer.group_id
    topics = [{topic, 0}, {topic, 1}]
    assert {:ok, committed} = Admin.list_consumer_group_offsets(ctx.admin, group_id, topics)
    assert committed == %{{topic, 0} => last_processed_offset_partition_0 + 1, {topic, 1} => nil}
  end

  @tag :require_kafka
  test "stop", ctx do
    mref = Process.monitor(ctx.admin)
    Admin.stop(ctx.admin)
    assert_receive {:DOWN, ^mref, :process, _pid, _reason}
  end
end
