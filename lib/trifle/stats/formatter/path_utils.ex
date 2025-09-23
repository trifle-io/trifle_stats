defmodule Trifle.Stats.Formatter.PathUtils do
  @moduledoc false

  @type path_segments :: [String.t()]

  @doc """
  Split path string into segments.
  """
  @spec split_path(String.t()) :: path_segments
  def split_path(path) when is_binary(path), do: String.split(path, ".", trim: true)

  @doc """
  Expand segments, resolving `*` wildcards to actual keys present in the values list.
  Returns a list of concrete path segments (no wildcards).
  """
  @spec resolve_paths([map()], path_segments) :: [path_segments]
  def resolve_paths(values_list, segments) when is_list(values_list) do
    segments
    |> expand(values_list, [])
    |> Enum.uniq()
  end

  @doc """
  Resolve segments, automatically expanding map targets when no wildcard is provided.
  """
  @spec resolve_concrete_paths([map()], path_segments) :: [path_segments]
  def resolve_concrete_paths(values_list, segments) do
    cond do
      has_wildcard?(segments) ->
        resolve_paths(values_list, segments)

      map_target?(values_list, segments) ->
        case resolve_paths(values_list, segments ++ ["*"]) do
          [] -> [segments]
          resolved -> resolved
        end

      true ->
        [segments]
    end
  end

  @doc """
  Fetch value from nested map using string segments.
  Tries string, atom, and integer keys for compatibility with stored data.
  """
  @spec fetch_path(map(), path_segments) :: any()
  def fetch_path(data, segments) do
    Enum.reduce_while(segments, data, fn segment, acc ->
      case fetch_segment(acc, segment) do
        {:ok, value} -> {:cont, value}
        :error -> {:halt, nil}
      end
    end)
  end

  defp expand([], _values_list, acc), do: [acc]

  defp expand([segment | rest], values_list, acc) do
    case segment do
      "*" ->
        keys = collect_keys(values_list, acc)

        keys
        |> Enum.flat_map(fn key ->
          expand(rest, values_list, acc ++ [key])
        end)

      _ ->
        expand(rest, values_list, acc ++ [segment])
    end
  end

  defp collect_keys(values_list, acc) do
    values_list
    |> Enum.flat_map(fn value ->
      case fetch_path(value, acc) do
        map when is_map(map) ->
          map
          |> Map.keys()
          |> Enum.map(&normalize_key/1)

        _ ->
          []
      end
    end)
    |> Enum.uniq()
  end

  defp fetch_segment(map, _segment) when not is_map(map), do: :error

  defp fetch_segment(map, segment) do
    segment_atom = safe_to_atom(segment)
    segment_int = maybe_integer(segment)

    with :error <- do_fetch(map, segment),
         :error <- do_fetch(map, segment_atom),
         :error <- do_fetch(map, segment_int) do
      :error
    end
  end

  defp has_wildcard?(segments), do: Enum.any?(segments, &(&1 == "*"))

  defp map_target?(values_list, segments) do
    Enum.any?(values_list, fn value ->
      case fetch_path(value, segments) do
        map when is_map(map) -> true
        _ -> false
      end
    end)
  end

  defp do_fetch(_map, nil), do: :error

  defp do_fetch(map, key) do
    case Map.fetch(map, key) do
      {:ok, value} -> {:ok, value}
      :error -> :error
    end
  end

  defp normalize_key(key) when is_atom(key), do: Atom.to_string(key)
  defp normalize_key(key) when is_binary(key), do: key
  defp normalize_key(key), do: to_string(key)

  defp safe_to_atom(segment) do
    try do
      String.to_atom(segment)
    rescue
      ArgumentError -> nil
    end
  end

  defp maybe_integer(segment) do
    case Integer.parse(segment) do
      {int, ""} -> int
      _ -> nil
    end
  end
end
