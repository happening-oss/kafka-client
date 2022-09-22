defmodule KafkaClient.Admin do
  @moduledoc """
  Kafka admin services.

  This module wraps the functions from the [Java Admin interface]
  (https://javadoc.io/static/org.apache.kafka/kafka-clients/3.2.3/org/apache/kafka/clients/admin/Admin.html).
  """

  use KafkaClient.GenPort
  alias KafkaClient.GenPort

  @spec start_link(servers: String.t()) :: {:ok, pid}
  def start_link(opts) do
    GenPort.start_link(
      __MODULE__,
      nil,
      "AdminPort",
      [%{"bootstrap.servers" => opts |> Keyword.fetch!(:servers) |> Enum.join(",")}],
      Keyword.take(opts, ~w/name/a)
    )
  end

  @doc "Synchronously stops the admin process."
  @spec stop(GenServer.server(), pos_integer | :infinity) :: :ok | {:error, :not_found}
  defdelegate stop(server, timeout \\ :infinity), to: GenPort

  @doc "Returns the list of partitions for the given topics."
  @spec describe_topics(GenServer.server(), [KafkaClient.topic()]) ::
          %{KafkaClient.topic() => [KafkaClient.partition()]}
  def describe_topics(server, topics), do: GenPort.call(server, :describe_topics, [topics])

  @impl GenServer
  def init(_), do: {:ok, nil}
end
