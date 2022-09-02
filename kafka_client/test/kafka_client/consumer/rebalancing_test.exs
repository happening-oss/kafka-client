defmodule KafkaClient.Consumer.RebalancingTest do
  use ExUnit.Case, async: true
  import KafkaClient.Test.Helper

  test "partitions lost notification" do
    group_id = unique("test_group")
    topics = start_consumer!(group_id: group_id).topics
    start_consumer!(group_id: group_id, topics: topics, recreate_topics?: false)
    assert_receive {:partitions_lost, _partitions}, :timer.seconds(30)
  end
end
