defmodule KafkaClient.Consumer.FlowTest do
  use ExUnit.Case, async: true
  import KafkaClient.Test.Helper

  test "clean termination" do
    group_id = unique("test_group")
    consumer = start_consumer!(group_id: group_id)

    [topic] = consumer.topics

    produce(topic, partition: 0)
    produce(topic, partition: 0)

    process_next_record!(topic, 0)
    processing_during_shutdown = assert_processing(topic, 0)

    os_pid = os_pid(port(consumer))
    assert os_process_alive?(os_pid)

    stop_consumer(consumer)
    refute Process.alive?(processing_during_shutdown.pid)

    Process.sleep(100)
    refute os_process_alive?(os_pid)

    start_consumer!(group_id: group_id, topics: consumer.topics, recreate_topics?: false)

    processing_after_shutdown = assert_processing(topic, 0)
    assert processing_after_shutdown.offset == processing_during_shutdown.offset
  end

  test "partitions lost notification" do
    group_id = unique("test_group")

    # using cooperative rebalance to avoid losing all partitions on rebalance
    consumer_params = %{
      "partition.assignment.strategy" =>
        "org.apache.kafka.clients.consumer.CooperativeStickyAssignor"
    }

    # start first consumer
    consumer1 = start_consumer!(group_id: group_id, consumer_params: consumer_params)

    [topic] = consumer1.topics

    # push some messages on both partitions
    produce(topic, partition: 0)
    produce(topic, partition: 0)
    produce(topic, partition: 0)

    produce(topic, partition: 1)
    produce(topic, partition: 1)
    produce(topic, partition: 1)

    # process one record on each partition
    process_next_record!(topic, 0)
    process_next_record!(topic, 1)

    # remember which are the next records being processed
    topic0_record_before_rebalance = assert_processing(topic, 0)
    topic1_record_before_rebalance = assert_processing(topic, 1)

    # start another consumer, this should trigger rebalance
    start_consumer!(
      group_id: group_id,
      topics: consumer1.topics,
      recreate_topics?: false,
      consumer_params: consumer_params
    )

    # await for the notification
    assert_receive {:unassigned, _partitions}, :timer.seconds(10)

    # check that topic 0 processor (consumer 1) is still running
    assert Process.alive?(topic0_record_before_rebalance.pid)
    resume_processing(topic0_record_before_rebalance)

    # check that the queue in consumer 1 is preserved (we should have one more message left)
    assert_processing(topic, 0)
    refute_processing(topic, 0)

    # check that the new consumer starts processing from a correct record
    topic1_record_after_rebalance = assert_processing(topic, 1)
    refute Process.alive?(topic1_record_before_rebalance.pid)
    assert topic1_record_after_rebalance.offset == topic1_record_before_rebalance.offset
  end

  test "handling of a port crash" do
    consumer = start_consumer!()
    port = port(consumer)

    ExUnit.CaptureLog.capture_log(fn ->
      mref = Process.monitor(consumer.pid)
      kill_port(port)
      assert_receive {:DOWN, ^mref, :process, _pid, :port_crash}
    end)
  end

  test "handling of a processor crash" do
    consumer = start_consumer!()
    [topic] = consumer.topics

    produce(topic, partition: 0)
    produce(topic, partition: 0)

    record = assert_processing(topic, 0)

    ExUnit.CaptureLog.capture_log(fn ->
      mref = Process.monitor(consumer.pid)
      Process.exit(record.pid, :kill)
      assert_receive {:DOWN, ^mref, :process, _pid, :processor_crashed}, :timer.seconds(10)
    end)
  end

  defp kill_port(port) do
    mref = Port.monitor(port)
    System.cmd("kill", ~w/-9 #{os_pid(port)}/)
    assert_receive {:DOWN, ^mref, _, _, _}
  end

  defp os_process_alive?(os_pid) do
    {_, status} = System.cmd("ps", ~w/-p #{os_pid}/)
    status == 0
  end
end
