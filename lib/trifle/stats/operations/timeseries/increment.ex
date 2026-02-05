defmodule Trifle.Stats.Operations.Timeseries.Increment do
  alias Trifle.Stats.Nocturnal.{Parser, Key}

  def perform(key, at, values, config \\ nil, opts \\ []) do
    storage = Trifle.Stats.Configuration.storage(config)
    tracking_key = tracking_key(opts)

    storage.__struct__.inc(
      Enum.map(config.granularities, fn granularity -> key_for(key, granularity, at, config) end),
      values,
      storage,
      tracking_key
    )
  end

  defp tracking_key(opts) do
    if Keyword.get(opts, :untracked, false), do: "__untracked__", else: nil
  end

  defp key_for(key, granularity, at, config) do
    parser = Parser.new(granularity)
    nocturnal = Trifle.Stats.Nocturnal.new(at, config)
    floored_at = Trifle.Stats.Nocturnal.floor(nocturnal, parser.offset, parser.unit)
    Key.new(key: key, granularity: granularity, at: floored_at)
  end
end
