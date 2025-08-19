defmodule Trifle.Stats.Formatter.Category do
  @moduledoc """
  Category formatter - transforms timeseries into category/histogram data.
  Groups data by category keys and accumulates values.
  """
  
  @behaviour Trifle.Stats.Formatter.Behaviour
  
  @impl true
  def format(series, path, slices \\ 1, transform_fn \\ nil) do
    if Enum.empty?(series[:at]) do
      []
    else
      string_keys = String.split(path, ".")
      atom_keys = string_keys |> Enum.map(&String.to_atom/1)
      
      # Zip timestamps with extracted values
      result = 
        series[:at]
        |> Enum.zip(series[:values])
        |> Enum.map(fn {at, values} ->
          extracted = get_in(values, atom_keys) || get_in(values, string_keys) || %{}
          {at, extracted}
        end)
      
      sliced_result = sliced(result, slices, transform_fn)
      if slices == 1, do: format_for_single_slice(hd(sliced_result), result), else: sliced_result
    end
  end
  
  defp sliced(result, slices, transform_fn) do
    count = length(result)
    slice_size = div(count, slices)
    start_index = count - (slice_size * slices)
    
    result
    |> Enum.drop(start_index)
    |> Enum.chunk_every(slice_size)
    |> Enum.map(fn slice ->
      slice
      |> Enum.reduce(%{}, fn {_at, data}, map ->
        case data do
          data when is_map(data) ->
            Enum.reduce(data, map, fn {key, value}, acc ->
              {k, v} = if transform_fn do
                transform_fn.(key, value)
              else
                {to_string(key), to_float(value)}
              end
              
              Map.update(acc, k, v, fn existing -> existing + v end)
            end)
          _ ->
            map
        end
      end)
    end)
  end
  
  defp format_for_single_slice(_category_map, result) do
    # Return timeline format for category formatter
    result
    |> Enum.map(fn {at, value} ->
      %{at: at, value: to_float(value)}
    end)
  end
  
  defp to_float(value) when is_number(value), do: value * 1.0
  defp to_float(_), do: 0.0
end