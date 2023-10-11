defmodule KafkaClient.Admin do
  @moduledoc """
  Kafka admin services.

  This module wraps the functions from the [Java Admin interface]
  (https://javadoc.io/static/org.apache.kafka/kafka-clients/3.2.3/org/apache/kafka/clients/admin/Admin.html).
  """

  use KafkaClient.GenPort
  alias KafkaClient.GenPort

  @spec start_link(servers: String.t(), name: GenServer.name(), admin_params: map()) :: {:ok, pid}
  def start_link(opts) do
    servers = Keyword.fetch!(opts, :servers)
    name = Keyword.get(opts, :name)
    java_opts = Keyword.get(opts, :admin_params, %{})

    startup_options = Map.merge(%{"bootstrap.servers" => Enum.join(servers, ",")}, java_opts)

    GenPort.start_link(
      __MODULE__,
      nil,
      "admin.Main",
      [startup_options],
      name: name
    )
  end

  @doc "Synchronously stops the admin process."
  @spec stop(GenServer.server(), timeout) :: :ok | {:error, :not_found}
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
  Returns the configuration of the given topics. Config is a list of topic parameters
  specified while creating a topic. This list can be found https://docs.confluent.io/platform/current/installation/configuration/topic-configs.html#ak-topic-configurations-for-cp
  """
  @spec describe_topics_config(GenServer.server(), KafkaClient.topic()) ::
          {:ok,
           %{
             KafkaClient.topic() => [
               %{name: String.t(), value: String.t(), is_default: boolean()}
             ]
           }}
          | {:error, String.t()}
  def describe_topics_config(server, topics),
    do: GenPort.call(server, :describe_topics_config, [topics])

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
  Returns the list of earliest offsets for the given topic partitions.
  If no record has been produced, the offset will be zero.
  """
  @spec list_earliest_offsets(GenServer.server(), [KafkaClient.topic_partition()]) ::
          {:ok, %{KafkaClient.topic_partition() => KafkaClient.offset()}}
          | {:error, String.t()}
  def list_earliest_offsets(server, topic_partitions),
    do: GenPort.call(server, :list_earliest_offsets, [topic_partitions])

  @doc """
  Returns the list of valid consumer groups in cluster
  Possible states defined here: https://kafka.apache.org/32/javadoc/org/apache/kafka/common/ConsumerGroupState.html
  """
  @spec list_consumer_groups(GenServer.server()) ::
          {:ok,
           [
             {String.t(), KafkaClient.consumer_state()}
           ]}
          | {:error, String.t()}
  def list_consumer_groups(server),
    do: GenPort.call(server, :list_consumer_groups, [])

  @doc """
  Returns a map of consumer groups with their descriptions.
  It describes to which topic/partition the consumer group is asseigned to
  as well as which client is connected to which partitions and what the group state is.`
  """
  @spec describe_consumer_groups(GenServer.server(), [String.t()]) ::
          {:ok,
           %{
             String.t() => %{
               members:
                 {String.t(),
                  [
                    {KafkaClient.topic(), KafkaClient.partition()}
                  ]},
               state: KafkaClient.consumer_state()
             }
           }}
          | {:error, String.t()}
  def describe_consumer_groups(server, consumer_groups),
    do: GenPort.call(server, :describe_consumer_groups, [consumer_groups])

  @doc """
  Deletes consumer groups.
  It will return list of deleted groups with :ok or :error tuples with message
  """
  @spec delete_consumer_groups(GenServer.server(), [String.t()]) ::
          {:ok, %{String.t() => :ok | {:error, String.t()}}} | {:error, String.t()}
  def delete_consumer_groups(server, consumer_groups),
    do: GenPort.call(server, :delete_consumer_groups, [consumer_groups])

  @doc """
  Returns committed offsets for the given consumer groups in the given partitions.

  Each returned offset is equal to the last committed offset incremented by 1. If the group does
  not have a committed offset for some partition, the corresponding value in the returned map will
  be 'nil'.
  """
  @spec list_consumer_group_offsets(
          GenServer.server(),
          %{String.t() => [KafkaClient.topic_partition()]}
        ) ::
          {:ok, %{String.t() => %{KafkaClient.topic_partition() => KafkaClient.offset() | nil}}}
          | {:error, String.t()}
  def list_consumer_group_offsets(server, group_specs),
    do: GenPort.call(server, :list_consumer_group_offsets, [group_specs])

  @doc "Creates the new topics."
  @spec create_topics(GenServer.server(), [
          KafkaClient.topic() | {KafkaClient.topic(), num_partitions :: pos_integer}
        ]) :: :ok | {:error, String.t()}
  def create_topics(server, topics) do
    GenPort.call(
      server,
      :create_topics,
      [Enum.map(topics, &with(name when is_binary(name) <- &1, do: {&1, nil}))]
    )
  end

  @doc "Deletes the given topics."
  @spec delete_topics(GenServer.server(), [KafkaClient.topic()]) :: :ok | {:error, String.t()}
  def delete_topics(server, topics),
    do: GenPort.call(server, :delete_topics, [topics])

  @impl GenServer
  def init(_), do: {:ok, nil}
end
