defmodule KafkaClient.Consumer.PortTest do
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

  test "handling of a port crash" do
    consumer = start_consumer!()
    port = port(consumer)

    ExUnit.CaptureLog.capture_log(fn ->
      kill_port(port)
      assert port(consumer) != port
    end)
  end

  defp os_process_alive?(os_pid) do
    {_, status} = System.cmd("ps", ~w/-p #{os_pid}/)
    status == 0
  end

  defp kill_port(port) do
    mref = Port.monitor(port)
    System.cmd("kill", ~w/-9 #{os_pid(port)}/)
    assert_receive {:DOWN, ^mref, _, _, _}
  end
end
