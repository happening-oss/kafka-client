# KafkaClient

## Compilation

1. Install asdf version manager and add the following plugins: elixir, erlang, java, maven
2. asdf install
3. mix deps.get
4. mix compile

## Running

1. Make sure there is a local kafka instance running on the port 9092.
2. `iex -S mix run -e TestConsumer.start`
3. In a separate terminal window publish some messages on the topic `mytopic`, and check that they are printed in the consumer window.
