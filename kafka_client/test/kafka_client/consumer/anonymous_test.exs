defmodule KafkaClient.Consumer.AnonymousTest do
  use ExUnit.Case, async: true
  import KafkaClient.Test.Helper

  test "multiple consumers" do
    topics = start_consumer!(group_id: nil, num_topics: 2).topics
    start_consumer!(group_id: nil, topics: topics, recreate_topics?: false)

    [topic1, topic2] = topics

    produced1 = produce(topic1, partition: 0)
    produced2 = produce(topic1, partition: 1)
    produced3 = produce(topic2, partition: 0)

    assert assert_processing(topic1, 0).offset == produced1.offset
    assert assert_processing(topic1, 0).offset == produced1.offset

    assert assert_processing(topic1, 1).offset == produced2.offset
    assert assert_processing(topic1, 1).offset == produced2.offset

    assert assert_processing(topic2, 0).offset == produced3.offset
    assert assert_processing(topic2, 0).offset == produced3.offset

    refute_processing(topic1, 0)
    refute_processing(topic1, 1)
    refute_processing(topic2, 0)

    start_consumer!(group_id: nil, topics: topics, recreate_topics?: false).topics

    assert assert_processing(topic1, 0).offset == produced1.offset
    assert assert_processing(topic1, 1).offset == produced2.offset
    assert assert_processing(topic2, 0).offset == produced3.offset
  end
end
