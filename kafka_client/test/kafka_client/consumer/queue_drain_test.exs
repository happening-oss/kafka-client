defmodule KafkaClient.Consumer.QueueDrainTest do
  use ExUnit.Case, async: true
  import KafkaClient.Test.Helper

  test "processing after the queue is drained" do
    consumer = start_consumer!()
    [topic] = consumer.topics

    produce(topic, partition: 0)
    produce(topic, partition: 0)
    produce(topic, partition: 0)
    topic |> assert_started_processing(0) |> resume_processing()
    topic |> assert_started_processing(0) |> resume_processing()
    topic |> assert_started_processing(0) |> resume_processing()

    assert buffers(consumer) == %{}

    produce(topic, partition: 0)
    topic |> assert_started_processing(0) |> resume_processing()
  end
end
