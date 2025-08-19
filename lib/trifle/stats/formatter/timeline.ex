defmodule Trifle.Stats.Formatter.Timeline do
  @moduledoc """
  Timeline formatter - transforms timeseries into timeline format.
  Preserves temporal ordering and returns [timestamp, value] pairs.
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
          extracted = get_in(values, atom_keys) || get_in(values, string_keys)
          {at, extracted}
        end)
      
      sliced_result = sliced(result, slices, transform_fn)
      if slices == 1, do: hd(sliced_result), else: sliced_result
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
      |> Enum.map(fn {at, value} ->
        if transform_fn do
          transform_fn.(at, value)
        else
          %{at: at, value: to_float(value)}
        end
      end)
    end)
  end
  
  defp to_float(value) when is_number(value), do: value * 1.0
  defp to_float(_), do: 0.0
end