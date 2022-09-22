defmodule KafkaClient.ConsumerTest do
  use ExUnit.Case, async: true

  import KafkaClient.Test.Helper

  test "named registration" do
    registered_name = unique_atom("consumer")
    consumer = start_named_consumer(registered_name)
    assert Process.whereis(registered_name) == consumer.pid
  end

  test "stop" do
    registered_name = unique_atom("consumer")
    consumer = start_named_consumer(registered_name)
    mref = Process.monitor(consumer.pid)
    KafkaClient.Consumer.stop(registered_name)
    assert_receive {:DOWN, ^mref, :process, _pid, _reason}
  end

  defp start_named_consumer(registered_name),
    do: start_consumer!(name: registered_name, recreate_topics: false, await_assigned?: false)
end
