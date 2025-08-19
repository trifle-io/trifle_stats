defmodule Trifle.Stats.Operations.Timeseries.Values do
  @moduledoc """
  Values operation - retrieves time-series data for a key over a time granularity.
  
  Supports skip_blanks parameter to filter out empty data points.
  """

  def perform(key, from, to, granularity, config \\ nil, skip_blanks \\ false) do
    timeline = Trifle.Stats.Nocturnal.timeline(from, to, granularity, config)
    values = config.driver.__struct__.get(
      Enum.map(timeline, fn at -> Trifle.Stats.Nocturnal.key_for(key, granularity, at, config) end),
      config.driver
    )
    
    result = %{at: timeline, values: values}
    
    if skip_blanks do
      clean_values(result)
    else
      result
    end
  end
  
  defp clean_values(%{at: timeline, values: values}) do
    timeline
    |> Enum.with_index()
    |> Enum.reduce(%{at: [], values: []}, fn {at, idx}, acc ->
      value = Enum.at(values, idx)
      
      # Skip empty values (empty maps are considered blank)
      if is_map(value) and map_size(value) == 0 do
        acc
      else
        %{
          at: acc.at ++ [at],
          values: acc.values ++ [value]
        }
      end
    end)
  end
end
