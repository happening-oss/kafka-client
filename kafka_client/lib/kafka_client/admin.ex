if Mix.env() in [:dev, :test] do
  defmodule KafkaClient.Admin do
    def recreate_topic(brokers, topic, opts \\ []) do
      if topic in topics(brokers),
        do: :ok = :brod.delete_topics(brokers, [topic], :timer.seconds(5))

      default_opts = %{num_partitions: 1, replication_factor: 1, assignments: [], configs: []}
      create_topic_info = default_opts |> Map.merge(Map.new(opts)) |> Map.put(:name, topic)

      recreate_topic_with_retry(brokers, create_topic_info)
    end

    defp recreate_topic_with_retry(brokers, create_topic_info, retries \\ 500) do
      case :brod.create_topics(brokers, [create_topic_info], %{timeout: :timer.seconds(5)}) do
        :ok ->
          :ok

        {:error, :topic_already_exists} ->
          if retries == 0, do: raise("timeout recreating topics")
          Process.sleep(10)
          recreate_topic_with_retry(brokers, create_topic_info, retries - 1)
      end
    end

    defp topics(brokers) do
      {:ok, meta} = :brod.get_metadata(brokers)
      Enum.map(meta.topics, & &1.name)
    end
  end
end
