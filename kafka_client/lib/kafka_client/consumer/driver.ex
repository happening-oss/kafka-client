defmodule KafkaClient.Consumer.Driver do
  @type instance :: any

  @callback open(consumer_params :: map, topics :: [String.t()], poll_interval :: pos_integer) ::
              instance
  @callback close(instance) :: :ok
end
