defmodule Trifle.Stats.Operations.Timeseries.Increment do
  def perform(key, at, values, config \\ nil) do
    config.driver.__struct__.inc(
      Enum.map(config.granularities, fn granularity -> key_for(key, granularity, at, config) end),
      values, config.driver
    )
  end

  def key_for(key, granularity, at, config) do
    Trifle.Stats.Nocturnal.key_for(key, granularity, at, config)
  end
end
