defmodule Trifle.Stats.Formatter.Category do
  @moduledoc """
  Category formatter - transforms timeseries into category/histogram data.
  Groups data by category keys and accumulates numeric totals keyed by
  fully-qualified path strings.
  """
  
  @behaviour Trifle.Stats.Formatter.Behaviour
  alias Trifle.Stats.Formatter.PathUtils
  
  @impl true
  def format(series, path, slices \\ 1, transform_fn \\ nil) do
    values_list = series[:values] || []

    if Enum.empty?(values_list) do
      %{}
    else
      string_keys = PathUtils.split_path(path)
      resolved_paths = PathUtils.resolve_concrete_paths(values_list, string_keys)

      sliced_result = sliced(values_list, resolved_paths, slices, transform_fn)

      if slices == 1, do: hd(sliced_result), else: sliced_result
    end
  end
  
  defp sliced(values_list, resolved_paths, slices, transform_fn) do
    count = length(values_list)
    slice_size = div(count, slices)
    start_index = count - (slice_size * slices)
    
    values_list
    |> Enum.drop(start_index)
    |> Enum.chunk_every(slice_size)
    |> Enum.map(fn slice -> aggregate_slice(slice, resolved_paths, transform_fn) end)
  end
  
  defp aggregate_slice(slice, resolved_paths, transform_fn) do
    Enum.reduce(slice, %{}, fn data, acc ->
      Enum.reduce(resolved_paths, acc, fn path_segments, inner_acc ->
        full_key = Enum.join(path_segments, ".")
        raw_value = PathUtils.fetch_path(data, path_segments)

        {key, numeric_value} =
          if transform_fn do
            case transform_fn.(full_key, raw_value) do
              {k, v} -> {to_string(k), v}
              other -> {full_key, other}
            end
          else
            {full_key, raw_value}
          end

        value = to_float(numeric_value)

        Map.update(inner_acc, key, value, &(&1 + value))
      end)
    end)
  end

  defp to_float(value) when is_number(value), do: value * 1.0
  defp to_float(_), do: 0.0
end
