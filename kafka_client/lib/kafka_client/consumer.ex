defmodule KafkaClient.Consumer do
  @moduledoc """
  Concurrent Kafka consumer.

  This module provides a higher-level implementation which should fit most typical consumer needs.
  In this implementation, records are processed in separate, partition-specific processes. This
  ensures that records on the same partition are processed sequentially, while separate partitions
  are processed concurrently.

  If the process model of this consumer doesn't fit your purposes, you can use the lower-level
  `KafkaClient.Consumer.Poller` abstraction.

  For usage details see `start_link/1` function. Also refer to the `Poller` documentation for
  the explanation of the common behaviour, such as load control, or telemetry.
  """

  use Parent.GenServer
  require Logger
  alias KafkaClient.Consumer.{PartitionProcessor, Poller}

  @type handler :: (Poller.notification() -> any)

  @doc """
  Starts the consumer process.

  This function takes all the same options as `Poller.start_link/1`, with one exception. Instead of
  the `:processor` option required by `Poller.start_link/1`, this function requires the `:handler`
  option, which is an anonymous function of arity 1, that will be invoked on every notification
  sent from the Kafka poller. The single argument passed to this function is of the type
  `t:Poller.notification/0`.

  Example:

      KafkaClient.Consumer.start_link(
        servers: ["localhost:9092"],
        group_id: "mygroup",
        topics: ["topic1", "topic2", ...],

        poll_duration: 10,
        commit_interval: :timer.seconds(5),

        # These parameters are passed directly to the Java client.
        # See https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html
        consumer_params: %{
          "heartbeat.interval.ms" => 100,
          "max.poll.interval.ms" => 1000,
        },

        handler: &handle_message/1
      )

  ## Concurrency consideration

  Messages on the same partition are processed sequentially. Messages on different partitions are
  processed concurrently. Internally, the consumer maintains one long-running process per each
  assigned partition. This process is started when the partition is assigned to the consumer, and
  it is terminated if the partition is unassigned.

  If the handler is invoked with the argument `{:record, record}`, the invocation takes place
  inside the partition process. All other handler invocations take place inside the main consumer
  process (which is the parent of the partitions processes). Avoid long processing, exceptions, and
  exits from these other handler invocations, because they might block the consumer or take it down
  completely. The `{:record, record}` handler invocation may run arbitrarily long, and it may
  safely throw an exception (see [Processing guarantees](#processing-guarantees)).

  ## Processing guarantees

  The consumer provides at-least-once processing guarantees, i.e. it is guaranteed that the
  `handler({:record, record})` invocation will finish at least once for each record. After the
  handler function finishes, the consumer will commit it to Kafka. This will also happen if the
  handler function throws an exception. This is done via `Poller.ack/1`.

  If you wish to handle the exception yourself, e.g. by retrying or republishing the message, you
  must catch the exception inside the handler function.

  If you wish to commit the record before it is processed, you can asynchronously send the record
  payload to another process, e.g. via `send` or `cast`, and then return from the handler function
  immediately. Alternatively, you can spawn another process to handle the message. This will change
  the processing guarantees to at-most-once, since it is possible that a record is committed, but
  never fully processed (e.g. the machine is taken down after the commits are flushed, but before
  the handler finishes).

  If the handler is spawning processes, they must be started somewhere else in the application
  supervision tree, not as direct children of the process where the handler is running (the
  partition process). For example, if you wish to handle the message asynchronously in a task, use
  `Task.Supervisor.start_child`, not `Task.start_link`. The latter may cause unexpected `:EXIT`
  messages, in which case the entire consumer will terminate. On the other hand, using `Task.async`
  with `Task.await` in the handler is fine, as long as you can be certain that tasks won't crash,
  or that the `await` won't time out.

  ### Shutdown and unassigned behaviour

  For efficiency reasons, the consumer aggregates pending commits, and submits them to the broker
  periodically. If a partition is unassigned, the consumer will attempt to flush all pending
  commits. The related partition process is then forcefully terminated. Likewise, on shutdown, the
  consumer will attempt to flush all pending commits on all topics, and forcefully terminate all
  partition processes. The consumer won't wait for the currently running handlers to finish, nor
  will it attempt to drain the current buffer (i.e. records already polled from Kafka). This
  approach reduces (but doesn't remove) the chance of a record being processed more than once.

  ## Telemetry

  In addition to telemetry events mentioned in the `Poller` docs, the consumer will emit the
  events for the handler invocation:

    - `kafka_client.consumer.record.handler.start.duration`
    - `kafka_client.consumer.record.handler.stop.duration`
    - `kafka_client.consumer.record.handler.exception.duration`
  """
  @spec start_link([Poller.option() | {:handler, handler}]) :: GenServer.on_start()
  def start_link(opts), do: Parent.GenServer.start_link(__MODULE__, opts)

  @impl GenServer
  def init(opts) do
    {handler, opts} = Keyword.pop!(opts, :handler)

    {:ok, poller} =
      Parent.start_child(
        {Poller, Keyword.put(opts, :processor, self())},
        id: :poller,
        restart: :temporary,
        ephemeral?: true
      )

    {:ok, %{handler: handler, poller: poller}}
  end

  @impl GenServer
  def handle_info({poller, message}, %{poller: poller} = state) do
    handle_poller_message(message, state)
    {:noreply, state}
  end

  @impl Parent.GenServer
  def handle_stopped_children(children, state) do
    crashed_children = Map.keys(children)
    {:stop, {:children_crashed, crashed_children}, state}
  end

  defp handle_poller_message({:assigned, partitions} = event, state) do
    start_processors(state.handler, partitions)
    state.handler.(event)
  end

  defp handle_poller_message({:unassigned, partitions} = event, state) do
    Enum.each(partitions, &Parent.shutdown_child({:processor, &1}))
    state.handler.(event)
  end

  defp handle_poller_message({:record, record}, _state) do
    {:ok, pid} = Parent.child_pid({:processor, {record.topic, record.partition}})
    PartitionProcessor.handle_record(pid, record)
  end

  defp handle_poller_message(message, state),
    do: state.handler.(message)

  defp start_processors(handler, partitions) do
    Enum.each(
      partitions,
      fn {topic, partition} ->
        {:ok, _pid} =
          Parent.start_child(
            {PartitionProcessor, handler},
            id: {:processor, {topic, partition}},
            restart: :temporary,
            ephemeral?: true,

            # We want to kill the processor immediately, and stop any currently running processor,
            # even if the processor is trapping exits.
            shutdown: :brutal_kill
          )
      end
    )
  end
end
