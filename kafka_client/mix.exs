defmodule KafkaClient.MixProject do
  use Mix.Project

  def project do
    [
      app: :kafka_client,
      version: "0.1.0",
      elixir: "~> 1.13",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    []
  end

  defp aliases do
    [
      compile: [&compile_port/1, "compile"]
    ]
  end

  defp compile_port(_args) do
    case System.cmd(
           "mvn",
           ~w/clean compile assembly:single/,
           cd: "port",
           stderr_to_stdout: true
         ) do
      {_output, 0} ->
        File.mkdir_p!("priv")

        File.cp(
          "port/target/kafka-client-1.0-SNAPSHOT-jar-with-dependencies.jar",
          "priv/kafka-client-1.0.jar"
        )

      {output, status} ->
        Mix.raise("#{output}\n\nPort project build exited with the status #{status}")
    end
  end
end
