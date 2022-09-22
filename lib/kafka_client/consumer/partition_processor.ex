defmodule KafkaClient.Consumer.PartitionProcessor do
  @moduledoc false

  # This module powers a process which handles a single assigned partition. See
  # `KafkaClient.Consumer` for details.
  #
  # This module is implemented as a hand-rolled OTP process, using `:proc_lib.start_link`.
  # This approach is chosen to support high-priority handling of the `drain` message. Basically
  # if the drain message is in the mailbox, it will be handled first, which means that the
  # processor will stop before starting to process the remaining messages. This is exactly the
  # behaviour we want from drain: wait until the processing of the current message is finished,
  # and then stop.
  #
  # See `next_message/1` for details.

  require Logger
  alias KafkaClient.Consumer.Poller

  @spec child_spec(KafkaClient.Consumer.handler()) :: Supervisor.child_spec()
  def child_spec(handler) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [handler]},
      type: :worker,
      # using brutal kill, because polite termination is supported via `drain/2`
      shutdown: :brutal_kill
    }
  end

  @spec start_link(KafkaClient.Consumer.handler()) :: {:ok, pid}
  def start_link(handler),
    do: :proc_lib.start_link(__MODULE__, :init, [self(), handler])

  @spec process_record(pid, Poller.record()) :: :ok
  def process_record(pid, record) do
    send(pid, {:process_record, record})
    :ok
  end

  @spec drain(pid, non_neg_integer) :: :ok | {:error, :timeout | any}
  def drain(pid, timeout) do
    mref = Process.monitor(pid)

    send(pid, :drain)

    receive do
      {:DOWN, ^mref, :process, ^pid, reason} ->
        if reason == :normal, do: :ok, else: {:error, reason}
    after
      timeout -> {:error, :timeout}
    end
  end

  @doc false
  def init(parent, handler) do
    :proc_lib.init_ack(parent, {:ok, self()})
    loop(parent, handler)
  end

  defp loop(parent, handler) do
    case next_message(parent) do
      :drain -> exit(:normal)
      {:EXIT, ^parent, reason} -> exit(reason)
      {:process_record, record} -> handle_record(record, handler)
      other -> Logger.warn("unknown message: #{inspect(other)}")
    end

    loop(parent, handler)
  end

  defp next_message(parent) do
    # Implements a so called "select receive" pattern, placing higher priority on drain and parent
    # exit messages. This allows us to first handle the high prio messages, even though they were
    # placed into the mailbox after the regular messages. This is the reason why we can't use
    # GenServer, but instead have to roll our own server process.

    receive do
      # try to fetch high prio messages from the mailbox if they exist
      :drain -> :drain
      {:EXIT, ^parent, _reason} = parent_exit -> parent_exit
    after
      # no high prio message in the mailbox, fetch the next message
      0 -> receive(do: (message -> message))
    end
  end

  defp handle_record(record, handler) do
    Poller.started_processing(record)

    try do
      :telemetry.span(
        [:kafka_client, :consumer, :record, :handler],
        %{},
        fn -> {handler.({:record, record}), Poller.telemetry_meta(record)} end
      )
    catch
      kind, payload when kind != :exit ->
        Logger.error(Exception.format(kind, payload, __STACKTRACE__))
    end

    Poller.ack(record)

    {:noreply, handler}
  end
end
