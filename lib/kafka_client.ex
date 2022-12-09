defmodule KafkaClient do
  @type topic :: String.t()
  @type partition :: non_neg_integer
  @type offset :: non_neg_integer
  @type timestamp :: non_neg_integer
  @type topic_partition :: {topic, partition}
  @type consumer_state ::
          :completing_rebalance
          | :dead
          | :empty
          | :preparing_rebalance
          | :stable
          | :unknown
          | :undefined
end
