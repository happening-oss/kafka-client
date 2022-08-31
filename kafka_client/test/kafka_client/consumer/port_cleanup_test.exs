defmodule KafkaClient.Consumer.PortCleanupTest do
  use ExUnit.Case, async: true
  import KafkaClient.Test.Helper

  test "port terminates if the owner process is stopped" do
    consumer = start_consumer!()
    os_pid = os_pid(port(consumer))
    assert os_process_alive?(os_pid)

    stop_consumer(consumer)

    Process.sleep(100)
    refute os_process_alive?(os_pid)
  end

  defp os_process_alive?(os_pid) do
    {_, status} = System.cmd("ps", ~w/-p #{os_pid}/)
    status == 0
  end
end
