defmodule KafkaClient.Admin do
  @moduledoc """
  Kafka admin services.

  This module wraps the functions from the [Java Admin interface]
  (https://javadoc.io/static/org.apache.kafka/kafka-clients/3.2.3/org/apache/kafka/clients/admin/Admin.html).
  """

  use KafkaClient.GenPort
  alias KafkaClient.GenPort

  @spec start_link(servers: String.t(), name: GenServer.name()) :: {:ok, pid}
  def start_link(opts) do
    GenPort.start_link(
      __MODULE__,
      nil,
      "admin.Main",
      [%{"bootstrap.servers" => opts |> Keyword.fetch!(:servers) |> Enum.join(",")}],
      Keyword.take(opts, ~w/name/a)
    )
  end

  @doc "Synchronously stops the admin process."
  @spec stop(GenServer.server(), pos_integer | :infinity) :: :ok | {:error, :not_found}
  defdelegate stop(server, timeout \\ :infinity), to: GenPort

  @doc "Returns the list of topics."
  @spec list_topics(GenServer.server()) :: [KafkaClient.topic()]
  def list_topics(server), do: GenPort.call(server, :list_topics)

  @doc "Returns the list of partitions for the given topics."
  @spec describe_topics(GenServer.server(), [KafkaClient.topic()]) ::
          {:ok, %{KafkaClient.topic() => [KafkaClient.partition()]}}
          | {:error, String.t()}
  def describe_topics(server, topics), do: GenPort.call(server, :describe_topics, [topics])

  @doc """
  Returns the list of end offsets for the given topic partitions.

  The end offset, also called high watermark, is the offset of the last produced message
  incremented by one. If no record has been produced, the offset will be zero.
  """
  @spec list_end_offsets(GenServer.server(), [KafkaClient.topic_partition()]) ::
          {:ok, %{KafkaClient.topic_partition() => KafkaClient.offset()}}
          | {:error, String.t()}
  def list_end_offsets(server, topic_partitions),
    do: GenPort.call(server, :list_end_offsets, [topic_partitions])

  @doc """
  Returns committed offsets for the given consumer group in the given partitions.

  Each returned offset is equal to the last committed offset incremented by 1. If the group does
  not have a committed offset for some partition, the corresponding value in the returned map will
  be 'nil'.
  """
  @spec list_consumer_group_offsets(
          GenServer.server(),
          String.t(),
          [KafkaClient.topic_partition()]
        ) ::
          {:ok, %{KafkaClient.topic_partition() => KafkaClient.offset() | nil}}
          | {:error, String.t()}
  def list_consumer_group_offsets(server, group_id, topic_partitions),
    do: GenPort.call(server, :list_consumer_group_offsets, [group_id, topic_partitions])

  @impl GenServer
  def init(_), do: {:ok, nil}
end
