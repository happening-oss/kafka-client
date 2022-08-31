defmodule KafkaClient.Consumer.PortCrashTest do
  use ExUnit.Case, async: true
  import KafkaClient.Test.Helper

  test "handling of a port crash" do
    consumer = start_consumer!()
    port = port(consumer)

    ExUnit.CaptureLog.capture_log(fn ->
      kill_port(port)
      assert port(consumer) != port
    end)
  end

  defp kill_port(port) do
    mref = Port.monitor(port)
    System.cmd("kill", ~w/-9 #{os_pid(port)}/)
    assert_receive {:DOWN, ^mref, _, _, _}
  end
end
