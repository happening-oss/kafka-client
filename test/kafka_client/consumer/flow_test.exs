defmodule KafkaClient.Consumer.FlowTest do
  use ExUnit.Case, async: true
  import KafkaClient.Test.Helper

  @moduletag :require_kafka

  test "clean termination" do
    group_id = unique("test_group")
    consumer = start_consumer!(group_id: group_id, drain: :timer.seconds(1), max_batch_size: 1)

    [topic] = consumer.subscriptions

    # produce some messages on paritions 0 and 1
    sync_produce!(topic, partition: 0)
    sync_produce!(topic, partition: 0)
    partition0_record3 = sync_produce!(topic, partition: 0)

    partition1_record1 = sync_produce!(topic, partition: 1)
    sync_produce!(topic, partition: 1)
    sync_produce!(topic, partition: 1)

    process_next_batch!(topic, 0)
    partition0_processing_during_shutdown = assert_processing(topic, 0)
    partition1_processing_during_shutdown = assert_processing(topic, 1)

    os_pid = os_pid(port(consumer))

    mref_consumer = Process.monitor(consumer.pid)
    mref_processor_partition0 = Process.monitor(partition0_processing_during_shutdown.processor)
    mref_processor_partition1 = Process.monitor(partition1_processing_during_shutdown.processor)

    # we need to stop the consumer from a separate process, to avoid blocking this process
    spawn(fn -> KafkaClient.Consumer.stop(consumer.pid) end)

    # sleep a bit to ensure that the termination is in progress, then instruct the processor
    # on partition 0 to resume processing the current message
    Process.sleep(500)
    resume_processing(partition0_processing_during_shutdown)

    # check that the consumer is down, and that the Java process stopped
    assert_receive {:DOWN, ^mref_consumer, :process, _pid, _reason}, :timer.seconds(2)
    refute os_process_alive?(os_pid)

    # the processor on partition 0 should have stopped normally, because it finished processing
    # the message in the given time
    assert_received {:DOWN, ^mref_processor_partition0, :process, _pid, :normal}

    # the processor on partition 1 should be brutally killed, because it didn't finish processing
    # the message in the given time
    assert_received {:DOWN, ^mref_processor_partition1, :process, _pid, :killed}

    # we'll now start another consumer in the same consumer group, and check that commits have
    # been flushed
    start_consumer!(group_id: group_id, subscriptions: [topic], recreate_topics?: false)

    # on partition 0 we should start with the 3rd record, since the first two have been processed
    partition0_processing_after_shutdown = assert_processing(topic, 0)
    assert hd(partition0_processing_after_shutdown.records).offset == partition0_record3.offset

    # on partition 1 we should start with the first record, since nothing was processed
    partition1_processing_after_shutdown = assert_processing(topic, 1)
    assert hd(partition1_processing_after_shutdown.records).offset == partition1_record1.offset
  end

  test "partitions lost notification" do
    group_id = unique("test_group")

    consumer_params = %{
      # shorten the heartbeat interval to avoid RebalanceInProgressException
      "heartbeat.interval.ms" => 100,
      # using cooperative rebalance to avoid losing all partitions on rebalance
      "partition.assignment.strategy" =>
        "org.apache.kafka.clients.consumer.CooperativeStickyAssignor"
    }

    # start first consumer
    consumer1 =
      start_consumer!(group_id: group_id, max_batch_size: 1, consumer_params: consumer_params)

    [topic] = consumer1.subscriptions

    # push some messages on both partitions
    sync_produce!(topic, partition: 0)
    sync_produce!(topic, partition: 0)
    sync_produce!(topic, partition: 0)

    sync_produce!(topic, partition: 1)
    sync_produce!(topic, partition: 1)
    sync_produce!(topic, partition: 1)

    # process one record on each partition
    process_next_batch!(topic, 0)
    process_next_batch!(topic, 1)

    # remember which are the next records being processed
    topic0_batch_before_rebalance = assert_processing(topic, 0)
    topic1_batch_before_rebalance = assert_processing(topic, 1)

    # start another consumer, this should trigger rebalance
    start_consumer!(
      group_id: group_id,
      subscriptions: consumer1.subscriptions,
      recreate_topics?: false,
      consumer_params: consumer_params,
      max_batch_size: 1
    )

    # await for the notification
    assert_receive {:unassigned, _partitions}, :timer.seconds(10)

    # sleep a bit more to let the rebalance finish (eliminates RebalanceInProgressException)
    Process.sleep(:timer.seconds(1))

    # check that topic 0 processor (consumer 1) is still running
    assert Process.alive?(topic0_batch_before_rebalance.processor)
    resume_processing(topic0_batch_before_rebalance)

    # check that the queue in consumer 1 is preserved (we should have one more message left)
    assert_processing(topic, 0)
    refute_processing(topic, 0)

    # check that the new consumer starts processing from a correct record
    topic1_batch_after_rebalance = assert_processing(topic, 1)
    refute Process.alive?(topic1_batch_before_rebalance.processor)

    assert hd(topic1_batch_after_rebalance.records).offset ==
             hd(topic1_batch_before_rebalance.records).offset
  end

  test "handling of a port crash" do
    consumer = start_consumer!()
    port = port(consumer)

    ExUnit.CaptureLog.capture_log(fn ->
      mref = Process.monitor(consumer.pid)
      kill_port(port)
      assert_receive {:DOWN, ^mref, :process, _pid, {:children_crashed, [:poller]}}
    end)
  end

  test "handling of a processor crash" do
    consumer = start_consumer!(max_batch_size: 1)
    [topic] = consumer.subscriptions

    sync_produce!(topic, partition: 0)
    sync_produce!(topic, partition: 0)

    batch = assert_processing(topic, 0)

    ExUnit.CaptureLog.capture_log(fn ->
      mref = Process.monitor(consumer.pid)
      Process.exit(batch.processor, :kill)
      assert_receive {:DOWN, ^mref, :process, _pid, reason}, :timer.seconds(10)
      assert reason == {:children_crashed, [{:processor, {topic, 0}}]}
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
