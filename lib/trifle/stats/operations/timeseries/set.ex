defmodule Trifle.Stats.Operations.Timeseries.Set do
  def perform(key, at, values, config \\ nil) do
    config.driver.__struct__.set(
      Enum.map(config.granularities, fn granularity -> key_for(key, granularity, at, config) end),
      values, config.driver
    )
  end

  def key_for(key, granularity, at, config) do
    Trifle.Stats.Nocturnal.key_for(key, granularity, at, config)
  end
end
