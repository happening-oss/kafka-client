defmodule KafkaClient.Consumer.QueueDrainTest do
  use ExUnit.Case, async: true
  import KafkaClient.Test.Helper

  test "processing after the queue is drained" do
    consumer = start_consumer!()
    [topic] = consumer.topics

    produce(topic, partition: 0)
    produce(topic, partition: 0)
    produce(topic, partition: 0)

    process_next_record!(topic, 0)
    process_next_record!(topic, 0)
    process_next_record!(topic, 0)

    assert buffers(consumer) == %{}

    produce(topic, partition: 0)
    process_next_record!(topic, 0)
  end
end
