defmodule Trifle.Stats.Aggregator.Max do
  @moduledoc """
  Max aggregator - finds maximum value with optional precision handling.
  
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
      
      sliced(result, slices)
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
    cond do
      count == 0 -> []
      slices <= 1 -> [max_list(result)]
      true ->
        slice_size = div(count, slices)
        if slice_size <= 0 do
          [max_list(result)]
        else
          start_index = count - (slice_size * slices)
          result
          |> Enum.drop(start_index)
          |> Enum.chunk_every(slice_size)
          |> Enum.map(&max_list/1)
        end
    end
  end

  defp max_list(slice) do
    compacted = Enum.reject(slice, &is_nil/1)
    case compacted do
      [] -> nil
      values ->
        max_val = Precision.max(values)
        if Precision.enabled?(), do: max_val, else: Precision.to_float(max_val)
    end
  end
end
