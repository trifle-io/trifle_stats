defmodule Trifle.Stats.Formatter.Timeline do
  @moduledoc """
  Timeline formatter - transforms timeseries into timeline format.
  Preserves temporal ordering and returns a map of path strings to
  `[at, value]` entry lists.
  """
  
  @behaviour Trifle.Stats.Formatter.Behaviour
  alias Trifle.Stats.Formatter.PathUtils
  
  @impl true
  def format(series, path, slices \\ 1, transform_fn \\ nil) do
    if Enum.empty?(series[:at]) do
      %{}
    else
      string_keys = PathUtils.split_path(path)
      resolved_paths = PathUtils.resolve_concrete_paths(series[:values], string_keys)
      zipped = Enum.zip(series[:at], series[:values])

      resolved_paths
      |> Enum.reduce(%{}, fn path_segments, acc ->
        full_key = Enum.join(path_segments, ".")

        result =
          Enum.map(zipped, fn {at, values} ->
            extracted = PathUtils.fetch_path(values, path_segments)
            {at, extracted}
          end)

        formatted = format_timeline(result, slices, transform_fn)

        Map.put(acc, full_key, formatted)
      end)
    end
  end
  
  defp format_timeline(result, slices, transform_fn) do
    sliced_result = sliced(result, slices, transform_fn)
    if slices == 1, do: hd(sliced_result), else: sliced_result
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
