defmodule KafkaClient.Consumer.RebalancingTest do
  use ExUnit.Case, async: true
  import KafkaClient.Test.Helper

  test "partitions lost notification" do
    consumer_params = %{"enable.auto.commit" => false}
    topics = start_consumer!(consumer_params: consumer_params).topics

    start_consumer!(topics: topics, recreate_topics?: false, consumer_params: consumer_params)

    assert_receive {:partitions_lost, _partitions}, :timer.seconds(30)
  end
end
