defmodule KafkaClient.Consumer.Driver.Test do
  use GenServer
  @behaviour KafkaClient.Consumer.Driver

  def produce(consumer, params \\ []) do
    default_record = %{
      topic: "topic_#{System.unique_integer([:positive, :monotonic])}",
      partition: System.unique_integer([:positive, :monotonic]),
      offset: System.unique_integer([:positive, :monotonic]),
      timestamp: DateTime.utc_now() |> DateTime.to_unix(:millisecond),
      payload: "payload#{System.unique_integer([:positive, :monotonic])}"
    }

    record = Map.merge(default_record, Map.new(params))

    driver_instance = :sys.get_state(consumer).driver_instance
    GenServer.call(driver_instance, {:produce, record})

    record
  end

  @impl KafkaClient.Consumer.Driver
  def open(_consumer_params, _topics, _poll_interval) do
    {:ok, pid} = GenServer.start_link(__MODULE__, self())
    pid
  end

  @impl KafkaClient.Consumer.Driver
  def close(pid), do: GenServer.stop(pid)

  @impl KafkaClient.Consumer.Driver
  def notify_processed(_port, _topic, _partition), do: :ok

  @impl GenServer
  def init(owner_pid), do: {:ok, owner_pid}

  @impl GenServer
  def handle_call({:produce, record}, _from, owner_pid) do
    {:record, record.topic, record.partition, record.offset, record.timestamp, record.payload}
    |> :erlang.term_to_binary()
    |> then(&send(owner_pid, {self(), {:data, &1}}))

    {:reply, :ok, owner_pid}
  end
end
