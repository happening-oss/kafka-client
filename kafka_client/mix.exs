defmodule KafkaClient.MixProject do
  use Mix.Project

  def project do
    [
      app: :kafka_client,
      version: "0.1.0",
      elixir: "~> 1.13",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases()
    ]
  end

  def application do
    [
      extra_applications: [:crypto, :logger]
    ]
  end

  defp elixirc_paths(env) when env in [:test], do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:brod, "~> 3.16", only: [:dev, :test]},
      {:dialyxir, "~> 1.2", only: [:dev, :test], runtime: false},
      {:parent, "~> 0.12"},
      {:telemetry, "~> 1.0"},
      {:statistex, "~> 1.0", only: [:dev, :test]}
    ]
  end

  defp aliases do
    [
      compile: [&compile_port/1, "compile"]
    ]
  end

  defp compile_port(_args) do
    if recompile_port?() do
      Mix.shell().info("Recompiling java port")
      src_hash = src_hash()

      case System.cmd(
             "mvn",
             ~w/clean compile assembly:single/,
             cd: "port",
             stderr_to_stdout: true
           ) do
        {_output, 0} ->
          File.write("port/target/src_hash", src_hash)
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

  defp recompile_port? do
    case File.read("port/target/src_hash") do
      {:ok, src_hash} -> src_hash != src_hash()
      {:error, _posix} -> true
    end
  end

  defp src_hash do
    src_files = ["port/pom.xml" | Path.wildcard("port/src/**/*.java")]
    :crypto.hash(:sha256, Enum.map(src_files, &File.read!/1))
  end
end
