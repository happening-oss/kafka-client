# This is a temp module used for local dev experiments, so we won't worry about credo here.
# credo:disable-for-this-file

defmodule TestConsumer do
  @moduledoc false
  def start do
    spawn(fn ->
      KafkaClient.Consumer.start_link(
        servers: ["localhost:9092"],
        group_id: "mygroup",
        subscriptions: ["mytopic"],
        handler: &IO.inspect/1
      )

      Process.sleep(:infinity)
    end)
  end
end
