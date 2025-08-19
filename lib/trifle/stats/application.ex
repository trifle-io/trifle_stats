defmodule Trifle.Stats.Application do
  @moduledoc """
  The Trifle.Stats application.
  
  Starts the dynamic registration system on application boot.
  """

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # Registry for dynamic aggregators/formatters/transponders
      Trifle.Stats.Series.Registry
    ]

    opts = [strategy: :one_for_one, name: Trifle.Stats.Supervisor]
    Supervisor.start_link(children, opts)
  end
end