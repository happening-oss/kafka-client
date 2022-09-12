defmodule KafkaClient.Consumer.Core do
  use GenServer
  require Logger
  alias KafkaClient.Consumer.Port

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @impl GenServer
  def init(opts) do
    Process.flag(:trap_exit, true)
    subscriber = Keyword.fetch!(opts, :subscriber)
    port = Port.open(opts)
    send(subscriber, {:port_started, port})
    {:ok, %{port: port, subscriber: subscriber}}
  end

  @impl GenServer
  def handle_info({port, {:data, data}}, %{port: port} = state) do
    send(state.subscriber, :erlang.binary_to_term(data))
    {:noreply, state}
  end

  def handle_info({port, {:exit_status, status}}, %{port: port} = state) do
    Logger.error("port exited with status #{status}")
    {:stop, :port_crash, %{state | port: nil}}
  end

  @impl GenServer
  def terminate(_reason, state),
    do: if(state.port != nil, do: Port.close(state.port))
end
