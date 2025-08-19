defmodule Trifle.Stats do
  @moduledoc """
  Simple timeline analytics for tracking custom metrics.
  
  Trifle.Stats allows you to track time-series data and status information with various storage backends.
  
  ## Configuration
  
  You can configure Trifle.Stats globally:
  
      # In your config/config.exs or application start
      driver = Trifle.Stats.Driver.Process.new()
      Trifle.Stats.configure(
        driver: driver,
        time_zone: "Europe/London",
        track_granularities: [:hour, :day, :week],
        beginning_of_week: :monday
      )
      
      # Then use without passing config to each call
      Trifle.Stats.track("page_views", DateTime.utc_now(), %{count: 1})
  
  Or pass configuration explicitly to each function call:
  
      config = Trifle.Stats.Configuration.configure(driver)
      Trifle.Stats.track("page_views", DateTime.utc_now(), %{count: 1}, config)
  """

  @doc """
  Configure Trifle.Stats globally. This is equivalent to calling
  `Trifle.Stats.Configuration.configure_global/1`.
  
  ## Examples
      driver = Trifle.Stats.Driver.Process.new()
      Trifle.Stats.configure(
        driver: driver,
        time_zone: "UTC",
        track_granularities: [:hour, :day, :month]
      )
  """
  def configure(opts) do
    Trifle.Stats.Configuration.configure_global(opts)
  end

  @doc """
  Track function to run icremental stats.

  ## Examples
      iex> Trifle.Stats.track("test", DateTime.utc_now(), %{duration: 103, count: 1})
      {:ok, []}
  """
  def track(key, at, values, config \\ nil) do
    resolved_config = Trifle.Stats.Configuration.resolve_config(config)
    Trifle.Stats.Operations.Timeseries.Increment.perform(
      key, at, values, resolved_config
    )
  end

  @doc """
  Assert function to run set stats.

  ## Examples
      iex> Trifle.Stats.assert("test", DateTime.utc_now(), %{duration: 103, count: 1})
      {:ok, []}
  """
  def assert(key, at, values, config \\ nil) do
    resolved_config = Trifle.Stats.Configuration.resolve_config(config)
    Trifle.Stats.Operations.Timeseries.Set.perform(
      key, at, values, resolved_config
    )
  end

  @doc """
  Values function to retrieve stats.

  ## Examples
      iex> Trifle.Stats.values("test", DateTime.utc_now(), DateTime.utc_now(), :day)
      %{at: [...], values: [...]}
      
      # Skip empty data points
      iex> Trifle.Stats.values("test", from, to, :day, config, skip_blanks: true)
      %{at: [...], values: [...]}  # Only non-empty entries
  """
  def values(key, from, to, granularity, config \\ nil, opts \\ []) do
    resolved_config = Trifle.Stats.Configuration.resolve_config(config)
    skip_blanks = Keyword.get(opts, :skip_blanks, false)
    
    Trifle.Stats.Operations.Timeseries.Values.perform(
      key, from, to, granularity, resolved_config, skip_blanks
    )
  end

  @doc """
  Assort function to run classification stats.

  ## Examples
      iex> Trifle.Stats.assort("test", DateTime.utc_now(), %{duration: 103})
      {:ok, []}
  """
  def assort(key, at, values, config \\ nil) do
    resolved_config = Trifle.Stats.Configuration.resolve_config(config)
    Trifle.Stats.Operations.Timeseries.Classify.perform(
      key, at, values, resolved_config
    )
  end

  @doc """
  Beam function to send status ping.

  ## Examples
      iex> Trifle.Stats.beam("test", DateTime.utc_now(), %{count: 5})
      {:ok, []}
  """
  def beam(key, at, values, config \\ nil) do
    resolved_config = Trifle.Stats.Configuration.resolve_config(config)
    Trifle.Stats.Operations.Status.Beam.perform(
      key, at, values, resolved_config
    )
  end

  @doc """
  Scan function to retrieve latest status.

  ## Examples
      iex> Trifle.Stats.scan("test")
      {:ok, %{}}
  """
  def scan(key, config \\ nil) do
    resolved_config = Trifle.Stats.Configuration.resolve_config(config)
    Trifle.Stats.Operations.Status.Scan.perform(
      key, resolved_config
    )
  end

  @doc """
  Series function to create a Series wrapper from values data.

  ## Examples
      iex> series_data = %{values: []}
      iex> series = Trifle.Stats.series(series_data)
      %Trifle.Stats.Series{series: %{values: []}}
  """
  def series(series_data) do
    Trifle.Stats.Series.new(series_data)
  end
end
