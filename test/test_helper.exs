if Mix.env() == :prod_test do
  ExUnit.start(exclude: [:require_kafka])
else
  KafkaClient.Test.Helper.initialize!()
  ExUnit.start()
end
