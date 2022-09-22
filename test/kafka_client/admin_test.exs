defmodule KafkaClient.AdminTest do
  use ExUnit.Case, async: true

  import KafkaClient.Test.Helper
  alias KafkaClient.Admin

  test "describe_topics" do
    topic1 = new_test_topic()
    topic2 = new_test_topic()

    recreate_topics([{topic1, num_partitions: 1}, {topic2, num_partitions: 2}])

    pid = start_supervised!({Admin, servers: ["localhost:9092"]})
    assert {:ok, topics} = Admin.describe_topics(pid, [topic1, topic2])
    assert topics == %{topic1 => [0], topic2 => [0, 1]}

    assert {:error, error} = Admin.describe_topics(pid, ["unknown_topic"])
    assert error == "This server does not host this topic-partition."
  end

  test "stop" do
    registered_name = unique_atom("admin")
    admin = start_supervised!({Admin, name: registered_name, servers: ["localhost:9092"]})
    mref = Process.monitor(admin)
    Admin.stop(registered_name)
    assert_receive {:DOWN, ^mref, :process, _pid, _reason}
  end
end
