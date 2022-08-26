defmodule TestConsumer do
  def start do
    spawn(fn ->
      KafkaClient.Consumer.start_link(
        servers: ["localhost:9092"],
        group_id: "mygroup",
        topics: ["mytopic"],
        handler: &IO.inspect/1
      )

      Process.sleep(:infinity)
    end)
  end
end
