defmodule KafkaClient.Consumer.Stream do
  @moduledoc """
  Kafka consumer as Elixir stream.
  """

  alias KafkaClient.Consumer.Poller

  @doc """
  Creates a stream of Kafka poller notifications.

  This function takes the same options as `Poller.start_link/1` (except the `:processor` option).
  It starts the poller as the direct child of the current process, and creates a stream of poller
  notifications of the type `t:Poller.notification/0`, such as `{:assigned, partitions}`,
  `{:record, record}`, ...

  This is an infinite stream that never stops on its own. If the client wants to stop the stream,
  it can use the standard `Stream` and `Enum` functions, such as `take`, `take_while`, or
  `reduce_while`. After the stream is closed the poller process is stopped.

  The client code is responsible for acknowledging when a record is processed. I.e. for every
  `{:record, record}` element the client must invoke `Poller.ack/1`. Failing to do so will cause
  the stream to hang, once all the buffers are full.

  Refer to the `Poller` documentation for explanation of the common behaviour, such as load
  control, or telemetry.

  Example:

      # Anonymous consuming (no consumer group)
      KafkaClient.Consumer.Stream.new([group_id: nil, ...])

      # Stop once all the records have been processed. For this to work, it is important to ack each record.
      |> Stream.take_while(&(&1 != :caught_up))

      # Take only the record notifications (i.e. ignore assigned/unassigned).
      |> Stream.filter(&match?({:record, record}, &1))

      # Process each record
      |> Enum.each(fn {:record, record} ->
        do_something_with(record)

        # don't forget to ack after the record is processed
        KafkaClient.Consumer.Poller.ack(record)
      end)

  If you're fine with sending the ack as soon as the record is received, you can wrap this in a
  helper function:

      def acked_records(opts) do
        opts
        |> KafkaClient.Consumer.Stream.new()
        |> Stream.take_while(&(&1 != :caught_up))
        |> Stream.filter(&match?({:record, record}, &1))
        |> Stream.map(fn {:record, record} -> record end)
      end
  """
  @spec new([Poller.option()]) :: Enumerable.t()
  def new(opts) do
    Stream.resource(
      fn -> start_poller(opts) end,
      &next_notification/1,
      &stop_poller/1
    )
  end

  defp start_poller(opts) do
    opts = Keyword.put(opts, :processor, self())
    {:ok, poller} = Poller.start_link(opts)
    mref = Process.monitor(poller)
    {poller, mref}
  end

  defp next_notification({poller, mref}) do
    receive do
      {^poller, notification} ->
        with {:record, record} <- notification, do: Poller.started_processing(record)
        {[notification], {poller, mref}}

      {:DOWN, _mref, :process, ^poller, reason} ->
        exit(reason)
    end
  end

  defp stop_poller({poller, mref}) do
    Process.demonitor(mref, [:flush])
    Poller.stop(poller)
    flush_messages(poller)
  end

  defp flush_messages(poller) do
    receive do
      {^poller, _notification} -> flush_messages(poller)
    after
      0 -> :ok
    end
  end
end
