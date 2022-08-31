defmodule KafkaClient.Consumer.Driver do
  @type instance :: any

  @callback open(consumer_params :: map, topics :: [String.t()], poll_interval :: pos_integer) ::
              instance
  @callback close(instance) :: :ok
  @callback notify_processed(instance, topic :: String.t(), partition :: non_neg_integer) :: :ok
end
