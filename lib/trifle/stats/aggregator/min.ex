defmodule Trifle.Stats.Aggregator.Min do
  @moduledoc """
  Min aggregator - finds minimum value with optional precision handling.
  
  Supports high-precision calculations using the Decimal library when precision mode
  is enabled via configuration.
  """
  
  @behaviour Trifle.Stats.Aggregator.Behaviour
  
  alias Trifle.Stats.Precision
  
  @impl true
  def aggregate(series, path, slices \\ 1) do
    if Enum.empty?(series[:at]) do
      []
    else
      keys = String.split(path, ".")
      result = 
        series[:values]
        |> Enum.map(&get_path_value(&1, keys))
      
      sliced_result = sliced(result, slices)
      if slices == 1, do: hd(sliced_result), else: sliced_result
    end
  end

  # Helper function to get value from path - handles both string and atom keys
  defp get_path_value(value_map, keys) do
    # Try string keys first (for real data)
    case get_in(value_map, keys) do
      nil ->
        # Fall back to atom keys (for test data)
        atom_keys = Enum.map(keys, &String.to_atom/1)
        get_in(value_map, atom_keys)
      value -> value
    end
  end
  
  defp sliced(result, slices) do
    count = length(result)
    slice_size = div(count, slices)
    start_index = count - (slice_size * slices)
    
    result
    |> Enum.drop(start_index)
    |> Enum.chunk_every(slice_size)
    |> Enum.map(fn slice ->
      compacted = Enum.reject(slice, &is_nil/1)
      case compacted do
        [] -> nil
        values -> 
          min_val = Precision.min(values)
          if Precision.enabled?(), do: min_val, else: Precision.to_float(min_val)
      end
    end)
  end
end