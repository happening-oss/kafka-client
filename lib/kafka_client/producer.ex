defmodule KafkaClient.Producer do
  use KafkaClient.GenPort
  alias KafkaClient.GenPort

  def start_link(opts) do
    servers = Keyword.fetch!(opts, :servers)
    user_producer_params = Keyword.get(opts, :producer_params, %{})

    producer_params =
      Map.merge(
        user_producer_params,
        %{
          "bootstrap.servers" => Enum.join(servers, ","),
          "key.serializer" => "org.apache.kafka.common.serialization.ByteArraySerializer",
          "value.serializer" => "org.apache.kafka.common.serialization.ByteArraySerializer"
        }
      )

    GenPort.start_link(
      __MODULE__,
      nil,
      "producer.Main",
      [producer_params],
      Keyword.take(opts, ~w/name/a)
    )
  end

  @doc "Synchronously stops the producer process."
  @spec stop(GenServer.server(), pos_integer | :infinity) :: :ok | {:error, :not_found}
  defdelegate stop(server, timeout \\ :infinity), to: GenPort

  def send(server, record),
    do: GenServer.call(server, {:send, record})

  @impl GenServer
  def init(_), do: {:ok, nil}

  @impl GenServer
  def handle_call({:send, record}, _from, state) do
    transport_record =
      %{key: nil, value: nil}
      |> Map.merge(record)
      |> Map.update!(:key, &{:__binary__, &1})
      |> Map.update!(:value, &{:__binary__, &1})

    GenPort.command(GenPort.port(), :send, [transport_record])
    {:reply, :ok, state}
  end
end
