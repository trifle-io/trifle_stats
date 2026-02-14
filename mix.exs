defmodule Trifle.Stats.MixProject do
  use Mix.Project

  def project do
    [
      app: :trifle_stats,
      version: "1.1.0",
      name: "Trifle.Stats",
      description: description(),
      package: package(),
      source_url: "https://github.com/trifle-io/trifle_stats",
      homepage_url: "https://trifle.io",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  defp description() do
    "Simplest timeline analytics."
  end

  defp package() do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/trifle-io/trifle_stats"}
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Trifle.Stats.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:jason, "~> 1.4.0"},
      {:mongodb_driver, "~> 1.2.0"},
      {:postgrex, ">= 0.17.0"},
      {:myxql, "~> 0.7.0"},
      {:redix, "~> 1.3.0"},
      {:tzdata, "~> 1.1.1"},
      # Override problematic SSL dependency with newer version compatible with OTP 28
      {:ssl_verify_fun, "~> 1.1.7", override: true},
      # SQLite support - trying exqlite instead of esqlite for better OTP 28 compatibility
      {:exqlite, "~> 0.20.0"},
      # Decimal library for high-precision arithmetic
      {:decimal, "~> 2.0"}
    ]
  end
end
