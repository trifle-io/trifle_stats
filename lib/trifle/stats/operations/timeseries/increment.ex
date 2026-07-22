defmodule Trifle.Stats.Operations.Timeseries.Increment do
  alias Trifle.Stats.Nocturnal.{Parser, Key}

  def perform(key, at, values, config \\ nil, opts \\ []) do
    driver = config.driver
    storage = Trifle.Stats.Configuration.storage(config)
    tracking_key = tracking_key(opts)

    if function_exported?(driver.__struct__, :direct_write, 6) do
      driver.__struct__.direct_write(:track, key, at, values, driver, opts)
    else
      storage.__struct__.inc(
        Enum.map(config.granularities, fn granularity -> key_for(key, granularity, at, config) end),
        values,
        storage,
        1,
        tracking_key
      )
    end
  end

  defp tracking_key(opts) do
    if Keyword.get(opts, :untracked, false), do: "__untracked__", else: nil
  end

  defp key_for(key, granularity, at, config) do
    parser = Parser.new(granularity)
    normalized_at = DateTime.shift_zone!(at, target_timezone(config))
    nocturnal = Trifle.Stats.Nocturnal.new(normalized_at, config)
    floored_at = Trifle.Stats.Nocturnal.floor(nocturnal, parser.offset, parser.unit)
    Key.new(key: key, granularity: granularity, at: floored_at)
  end

  defp target_timezone(config) do
    if config && config.time_zone, do: config.time_zone, else: "Etc/UTC"
  end
end
