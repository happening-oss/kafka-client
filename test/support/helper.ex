defmodule KafkaClient.Test.Helper do
  import ExUnit.Assertions
  alias KafkaClient.{Admin, Producer}

  def eventually(fun, opts \\ []),
    do: eventually(fun, Keyword.get(opts, :attempts, 10), Keyword.get(opts, :delay, 100))

  def unique(prefix), do: "#{prefix}_#{System.unique_integer([:positive, :monotonic])}"

  def unique_atom(prefix) do
    # Can't avoid dynamic atom creation, and this is used only in tests, so disabling the check
    # credo:disable-for-next-line Credo.Check.Warning.UnsafeToAtom
    :"#{unique(prefix)}"
  end

  def initialize! do
    Producer.start_link(
      servers: servers(),
      name: :test_producer
    )

    Admin.start_link(servers: KafkaClient.Test.Helper.servers(), name: :test_admin)

    # delete test topics from a previous `mix test` session, which can occur if the test OS process
    # has been forcefully terminated (e.g. via ctrl+c)
    all_topics = KafkaClient.Admin.list_topics(:test_admin)
    test_topics = Enum.filter(all_topics, &String.starts_with?(&1, "kafka_client_test_"))
    if test_topics != [], do: Admin.delete_topics(:test_admin, test_topics)
  end

  def start_consumer!(opts \\ []) do
    group_id = Keyword.get(opts, :group_id, unique("test_group"))

    subscriptions = subscriptions(opts)

    test_pid = self()
    child_id = make_ref()

    opts =
      [
        servers: servers(),
        group_id: group_id,
        subscriptions: subscriptions,
        handler: &handle_consumer_event(&1, test_pid),
        commit_interval: 50,
        consumer_params: Keyword.get(opts, :consumer_params, %{}),
        drain: Keyword.get(opts, :drain, 0)
      ] ++ Keyword.take(opts, ~w/name/a)

    consumer_pid =
      ExUnit.Callbacks.start_supervised!(
        {KafkaClient.Consumer, opts},
        id: child_id,
        restart: :temporary
      )

    handler_id = make_ref()

    :telemetry.attach(
      handler_id,
      [:kafka_client, :consumer, :record, :queue, :start],
      fn _name, _measurements, meta, _config ->
        if hd(Process.get(:"$ancestors")) == consumer_pid, do: send(test_pid, {:polled, meta})
      end,
      nil
    )

    ExUnit.Callbacks.on_exit(fn -> :telemetry.detach(handler_id) end)

    if Keyword.get(opts, :await_assigned?, true),
      do: assert_receive({:assigned, _partitions}, :timer.seconds(10))

    %{pid: consumer_pid, child_id: child_id, subscriptions: subscriptions, group_id: group_id}
  end

  def stop_consumer(consumer), do: ExUnit.Callbacks.stop_supervised(consumer.child_id)

  def produce(topic, opts \\ []) do
    record = record(topic, opts)
    Producer.send(:test_producer, record)
    record
  end

  def sync_produce(topic, opts \\ []) do
    record = record(topic, opts)

    [result] = Producer.sync_send(:test_producer, [record], :infinity) |> Enum.to_list()
    result
  end

  def sync_produce!(topic, opts \\ []) do
    {:ok, result} = sync_produce(topic, opts)
    result
  end

  def resume_processing(record) do
    send(record.pid, :consume)
    %{topic: topic, partition: partition, offset: offset} = record
    assert_receive {:processed, ^topic, ^partition, ^offset}
    :ok
  end

  def crash_processing(record, reason) do
    send(record.pid, {:crash, reason})
    :ok
  end

  def assert_polled(topic, partition, offset) do
    assert_receive {:polled, %{topic: ^topic, partition: ^partition, offset: ^offset}},
                   :timer.seconds(10)
  end

  def refute_polled(topic, partition, offset) do
    refute_receive {:polled, %{topic: ^topic, partition: ^partition, offset: ^offset}},
                   :timer.seconds(1)
  end

  def assert_processing(topic, partition) do
    assert_receive {:processing, %{topic: ^topic, partition: ^partition} = record},
                   :timer.seconds(10)

    record
  end

  def refute_processing(topic, partition) do
    refute_receive {:processing, %{topic: ^topic, partition: ^partition}}
  end

  def assert_caught_up, do: assert_receive(:caught_up, :timer.seconds(10))
  def refute_caught_up, do: refute_receive(:caught_up, :timer.seconds(1))

  def process_next_record!(topic, partition) do
    record = assert_processing(topic, partition)
    resume_processing(record)
    record
  end

  def poller(consumer) do
    {:ok, poller} = Parent.Client.child_pid(consumer.pid, :poller)
    poller
  end

  def port(consumer) do
    {:dictionary, dictionary} = Process.info(poller(consumer), :dictionary)
    dictionary |> Map.new() |> Map.fetch!({KafkaClient.GenPort, :port})
  end

  def os_pid(port) do
    {:os_pid, os_pid} = Port.info(port, :os_pid)
    os_pid
  end

  def servers, do: Enum.map(brokers(), fn {host, port} -> "#{host}:#{port}" end)

  def new_test_topic do
    # The approach below has been chosen over `System.to_integer` to reduce the chance of a name
    # collision between two `mix test` session, which eliminates some weird race conditions if a
    # topic is deleted and then immediately recreated.
    [
      "kafka_client_test_topic",
      # current time reduces the chance of a name collision between two `mix test` sessions
      DateTime.utc_now() |> DateTime.to_unix(:microsecond) |> to_string(),

      # randomness reduces the change of a name collision in a single `mix test` session
      :crypto.strong_rand_bytes(4) |> Base.url_encode64(padding: false)
    ]
    |> Enum.join("_")
  end

  def recreate_topics(topics) do
    topics = Enum.map(topics, &with(name when is_binary(name) <- &1, do: {&1, 2}))
    :ok = Admin.create_topics(:test_admin, topics)
    topic_names = Enum.map(topics, &elem(&1, 0))
    ExUnit.Callbacks.on_exit(fn -> Admin.delete_topics(:test_admin, topic_names) end)
  end

  defp record(topic, opts) do
    headers = for _ <- 1..5, do: {unique("header_key"), :crypto.strong_rand_bytes(4)}
    key = Keyword.get(opts, :key, :crypto.strong_rand_bytes(4))
    value = Keyword.get(opts, :value, :crypto.strong_rand_bytes(4))

    timestamp =
      DateTime.to_unix(~U[2022-01-01 00:00:00.00Z], :nanosecond) +
        System.unique_integer([:positive, :monotonic])

    partition = Keyword.get(opts, :partition, 0)

    %{
      topic: topic,
      key: key,
      value: value,
      timestamp: timestamp,
      partition: partition,
      headers: headers
    }
  end

  defp subscriptions(opts) do
    subscriptions =
      Keyword.get_lazy(
        opts,
        :subscriptions,
        fn ->
          Enum.map(
            1..Keyword.get(opts, :num_topics, 1)//1,
            fn _ -> new_test_topic() end
          )
        end
      )

    if Keyword.get(opts, :recreate_topics?, true) do
      subscriptions
      |> Enum.map(
        &{with({topic, _partition} <- &1, do: topic), Keyword.get(opts, :num_topics, 2)}
      )
      |> recreate_topics()
    end

    subscriptions
  end

  defp handle_consumer_event({event_name, _} = event, test_pid)
       when event_name in ~w/assigned unassigned polled committed/a,
       do: send(test_pid, event)

  defp handle_consumer_event(:caught_up, test_pid), do: send(test_pid, :caught_up)

  defp handle_consumer_event({:record, record}, test_pid) do
    send(test_pid, {:processing, Map.put(record, :pid, self())})

    receive do
      :consume -> :ok
      {:crash, reason} -> raise reason
    end

    send(test_pid, {:processed, record.topic, record.partition, record.offset})
  end

  defp brokers, do: [{"localhost", 9092}]

  defp eventually(fun, attempts, delay) do
    fun.()
  rescue
    e in [ExUnit.AssertionError] ->
      if attempts == 1, do: reraise(e, __STACKTRACE__)
      Process.sleep(delay)
      eventually(fun, attempts - 1, delay)
  end
end
