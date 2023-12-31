defmodule KafkaClient.GenPort do
  @moduledoc false

  # Elixir part of a generic port implementation. This module is meant to be used with a Java port
  # powered by the `PortDriver` class. For example implementations, see `KafkaClient.Admin` or
  # `KafkaClient.Consumer.Poller`.
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

  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts, behaviour: __MODULE__] do
      opts = Keyword.put_new(opts, :shutdown, :infinity)

      use GenServer, opts
      @behaviour behaviour

      @impl behaviour
      def handle_port_message(message, state) do
        require Logger

        Logger.warning("unhandled port message #{inspect(message)}")
        {:noreply, state}
      end

      defoverridable handle_port_message: 2
    end
  end

  @spec start_link(module, any, String.t(), [term], name: GenServer.name()) :: {:ok, pid}
  def start_link(callback, callback_arg, main_class, port_args, gen_server_opts \\ []) do
    GenServer.start_link(
      __MODULE__,
      {callback, callback_arg, main_class, port_args},
      gen_server_opts
    )
  end

  @spec stop(GenServer.server(), timeout) :: :ok | {:error, :not_found}
  def stop(server, timeout \\ :infinity) do
    case GenServer.whereis(server) do
      pid when is_pid(pid) -> GenServer.stop(server, :normal, timeout)
      nil -> {:error, :not_found}
    end
  end

  @spec command(port, atom, [term], String.t() | nil) :: :ok
  def command(port, name, args \\ [], ref \\ nil) do
    Port.command(port, :erlang.term_to_binary({name, args, ref}))
    :ok
  end

  @spec port :: port | nil
  def port, do: Process.get({__MODULE__, :port})

  @spec call(GenServer.server(), atom, [term], timeout) :: term
  def call(server, name, args \\ [], timeout \\ :timer.seconds(10)) do
    server
    |> GenServer.whereis()
    |> GenServer.call({{__MODULE__, :call}, name, args}, timeout)
  end

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
  def handle_info({port, {:exit_status, status}}, state) when is_port(port) do
    Logger.error("unexpected port exit with status #{status}")
    Process.delete({__MODULE__, :port})
    {:stop, :port_crash, state}
  end

  @impl GenServer
  def handle_info({port, {:data, data}}, state) when is_port(port) do
    term = :erlang.binary_to_term(data)

    if handle_special_port_message(term),
      do: {:noreply, state},
      else: callback().handle_port_message(term, state)
  end

  def handle_info(message, state),
    do: callback().handle_info(message, state)

  @impl GenServer
  def handle_call({{__MODULE__, :call}, name, args}, from, state) do
    port = port()
    ref = store_call(from)
    command(port, name, args, ref)
    {:noreply, state}
  end

  def handle_call(message, from, state), do: callback().handle_call(message, from, state)

  @impl GenServer
  def handle_cast(message, state), do: callback().handle_cast(message, state)

  defp handle_special_port_message({:"$kafka_consumer_response", ref, reply}) do
    {from, calls} = Map.pop!(calls(), ref)
    Process.put({__MODULE__, :calls}, calls)
    GenServer.reply(from, reply)
    true
  end

  defp handle_special_port_message({:metrics, transfer_time, duration}) do
    transfer_time = System.convert_time_unit(transfer_time, :nanosecond, :native)
    duration = System.convert_time_unit(duration, :nanosecond, :native)

    :telemetry.execute(
      [:kafka_client, :consumer, :port, :stop],
      %{
        system_time: System.system_time(),
        transfer_time: transfer_time,
        duration: duration
      },
      %{}
    )

    true
  end

  defp handle_special_port_message(_other), do: nil

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
          "com.happening.kafka.#{main_class}" | encoded_args
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

  defp callback, do: Process.get({__MODULE__, :callback})

  defp store_call({_pid, tag} = from) do
    ref = tag |> :erlang.term_to_binary() |> Base.encode64(padding: false)
    Process.put({__MODULE__, :calls}, Map.put(calls(), ref, from))
    ref
  end

  defp calls, do: Process.get({__MODULE__, :calls}, %{})
end
