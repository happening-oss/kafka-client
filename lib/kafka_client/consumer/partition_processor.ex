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
  alias KafkaClient.Consumer
  alias KafkaClient.Consumer.Poller

  @spec child_spec({Consumer.handler(), Consumer.batch_size()}) :: Supervisor.child_spec()
  def child_spec({handler, max_batch_size}) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [handler, max_batch_size]},
      type: :worker,
      # using brutal kill, because polite termination is supported via `drain/2`
      shutdown: :brutal_kill
    }
  end

  @spec start_link(Consumer.handler(), Consumer.batch_size()) :: {:ok, pid}
  def start_link(handler, max_batch_size),
    do: :proc_lib.start_link(__MODULE__, :init, [self(), handler, max_batch_size])

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
  def init(parent, handler, max_batch_size) do
    :proc_lib.init_ack(parent, {:ok, self()})
    loop(parent, handler, max_batch_size)
  end

  defp loop(parent, handler, max_batch_size) do
    case next_message(parent) do
      {:process_record, record} ->
        # the batch consists of all the records currently residing in the mailbox
        batch = [record | records_in_mailbox(parent, max_batch_size, 1)]
        handle_records(batch, handler)

      other ->
        Logger.warn("unknown message: #{inspect(other)}")
    end

    loop(parent, handler, max_batch_size)
  end

  defp records_in_mailbox(_parent, max_batch_size, max_batch_size), do: []

  defp records_in_mailbox(parent, max_batch_size, batch_size) do
    # takes all records in the mailbox
    case next_message(parent, _timeout = 0) do
      {:process_record, record} ->
        [record | records_in_mailbox(parent, max_batch_size, batch_size + 1)]

      nil ->
        []

      other ->
        Logger.warn("unknown message: #{inspect(other)}")
        []
    end
  end

  defp next_message(parent, timeout \\ :infinity) do
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
      0 ->
        receive do
          message -> message
        after
          timeout -> nil
        end
    end
    |> case do
      :drain -> exit(:normal)
      {:EXIT, ^parent, reason} -> exit(reason)
      other -> other
    end
  end

  defp handle_records(records, handler) do
    Enum.each(records, &Poller.started_processing/1)

    try do
      handler.({:records, records})

      # TODO: fix telemetry later
      # :telemetry.span(
      #   [:kafka_client, :consumer, :record, :handler],
      #   %{},
      #   fn -> {handler.({:record, record}), Poller.telemetry_meta(record)} end
      # )
    catch
      kind, payload when kind != :exit ->
        Logger.error(Exception.format(kind, payload, __STACKTRACE__))
    end

    # TODO: send all acks at once
    Enum.each(records, &Poller.ack/1)

    {:noreply, handler}
  end
end
