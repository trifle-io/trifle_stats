defmodule Trifle.Stats.Operations.Timeseries.Set do
  alias Trifle.Stats.Nocturnal.{Parser, Key}

  def perform(key, at, values, config \\ nil) do
    storage = Trifle.Stats.Configuration.storage(config)

    storage.__struct__.set(
      Enum.map(config.granularities, fn granularity -> key_for(key, granularity, at, config) end),
      values,
      storage
    )
  end

  defp key_for(key, granularity, at, config) do
    parser = Parser.new(granularity)
    nocturnal = Trifle.Stats.Nocturnal.new(at, config)
    floored_at = Trifle.Stats.Nocturnal.floor(nocturnal, parser.offset, parser.unit)
    Key.new(key: key, granularity: granularity, at: floored_at)
  end
end
