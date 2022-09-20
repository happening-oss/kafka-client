defmodule KafkaClient.GenPort do
  @moduledoc false

  # Elixir part of a generic port implementation. This module is meant to be used with a Java port
  # powered by the `PortDriver` class. For example, see `KafkaClient.Consumer.Poller`.
  #
  # Basic usage:
  #
  # 1. `use KafkaClient.GenPort`. This implicitly injects `use GenServer` to the module, i.e.
  #    the callback module will also be a GenServer.
  # 2. Implement the `handle_port_message` callback.
  # 3. Implement other GenServer callbacks, such as `init/1`, `handle_call`, etc.

  use GenServer
  require Logger

  @type state :: any

  @callback handle_port_message(message :: any, state) ::
              {:noreply, state}
              | {:noreply, state, timeout() | :hibernate | {:continue, continue_arg :: term()}}
              | {:stop, reason :: term(), state}

  def start_link(callback, callback_arg, main_class, port_args, gen_server_opts \\ []) do
    GenServer.start_link(
      __MODULE__,
      {callback, callback_arg, main_class, port_args},
      gen_server_opts
    )
  end

  def command(port, payload) do
    unless is_atom(payload) or (is_tuple(payload) and is_atom(elem(payload, 0))),
      do: raise("payload can only be an atom or a tuple where the first element is atom")

    payload = with atom when is_atom(atom) <- payload, do: {payload}
    Port.command(port, :erlang.term_to_binary(payload))

    :ok
  end

  def port(), do: Process.get({__MODULE__, :port})

  @impl GenServer
  def init({callback, callback_arg, main_class, port_args}) do
    Process.flag(:trap_exit, true)

    port = open(main_class, port_args)
    Process.put({__MODULE__, :port}, port)

    with {:ok, user_state} <- callback.init(callback_arg) do
      Process.put({__MODULE__, :callback}, callback)
      {:ok, user_state}
    end
  end

  @impl GenServer
  def terminate(reason, state) do
    callback().terminate(reason, state)
    if port() != nil, do: close()
  end

  @impl GenServer
  def handle_info({port, {:exit_status, status}} = message, state) do
    if port == port() do
      Logger.error("unexpected port exit with status #{status}")
      {:stop, :port_crash, %{state | port: nil}}
    else
      callback().handle_info(message, state)
    end
  end

  @impl GenServer
  def handle_info({port, {:data, data}} = message, state) do
    if port == port(),
      do: data |> :erlang.binary_to_term() |> callback().handle_port_message(state),
      else: callback().handle_info(message, state)
  end

  def handle_info(message, state),
    do: callback().handle_info(message, state)

  @impl GenServer
  def handle_cast(message, state), do: callback().handle_cast(message, state)

  @impl GenServer
  def handle_call(message, from, state), do: callback().handle_call(message, from, state)

  defp open(main_class, args) do
    encoded_args = Enum.map(args, &(&1 |> :erlang.term_to_binary() |> Base.encode64()))

    Port.open(
      {:spawn_executable, System.find_executable("java")},
      [
        :nouse_stdio,
        :binary,
        :exit_status,
        packet: 4,
        args: [
          "-cp",
          "#{Application.app_dir(:kafka_client)}/priv/kafka-client-1.0.jar",
          "com.superology.kafka.#{main_class}" | encoded_args
        ]
      ]
    )
  end

  defp close do
    port = port()
    command(port, :stop)

    receive do
      {^port, {:exit_status, 0}} -> :ok
      {^port, {:exit_status, status}} -> {:error, status}
    after
      :timer.seconds(5) -> {:error, :timeout}
    end
  end

  defp callback(), do: Process.get({__MODULE__, :callback})

  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts, behaviour: __MODULE__] do
      opts = Keyword.put_new(opts, :shutdown, :infinity)

      use GenServer, opts
      @behaviour behaviour
    end
  end
end
