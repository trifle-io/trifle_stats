defmodule Trifle.Stats.MixProject do
  use Mix.Project

  def project do
    [
      app: :trifle_stats,
      version: "1.0.1",
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
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false},
      {:mongodb_driver, "~> 1.2.0"},
      {:tzdata, "~> 1.1.1"}
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
    ]
  end
end
