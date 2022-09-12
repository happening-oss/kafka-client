defmodule KafkaClient.Consumer.Stream do
  alias KafkaClient.Consumer.Poller

  def new(opts) do
    Stream.resource(
      fn ->
        opts = Keyword.put(opts, :subscriber, self())
        {:ok, poller} = Poller.start_link(opts)
        Process.monitor(poller)
        poller
      end,
      fn poller ->
        receive do
          {^poller, message} ->
            with {:record, record} <- message, do: Poller.started_processing(record)
            {[message], poller}

          {:DOWN, _mref, :process, ^poller, reason} ->
            exit(reason)
        end
      end,
      &Poller.stop/1
    )
  end
end
