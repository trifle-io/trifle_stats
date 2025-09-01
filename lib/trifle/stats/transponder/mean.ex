defmodule Trifle.Stats.Transponder.Mean do
  @moduledoc """
  Mean transponder - calculates the arithmetic mean of scalar values from multiple paths.

  This transponder takes an array of paths, extracts scalar values from each path,
  and computes their average. Only numeric values are included in the calculation.

  ## Usage

      # Calculate mean of scalar values from multiple paths
      Trifle.Stats.Transponder.Mean.transform(series, ["requests.items", "requests.books"], "requests.average")

  ## Input Format

  Expected data structure:
      %{
        at: [timestamp1, timestamp2, ...],
        values: [
          %{"requests" => %{"items" => 10, "books" => 20, "shoes" => 30}},
          %{"requests" => %{"items" => 15, "books" => 25, "shoes" => 35}},
          ...
        ]
      }

  ## Output Format

  Returns modified series with mean values added:
      %{
        at: [timestamp1, timestamp2, ...],
        values: [
          %{"requests" => %{"items" => 10, "books" => 20, "shoes" => 30, "average" => 15.0}},
          %{"requests" => %{"items" => 15, "books" => 25, "shoes" => 35, "average" => 20.0}},
          ...
        ]
      }
  """

  @behaviour Trifle.Stats.Transponder.Behaviour

  alias Trifle.Stats.Precision

  @impl true
  def transform(series, paths, response_path, _unused_param \\ nil, _slices \\ 1) do
    if Enum.empty?(series[:at]) do
      series
    else
      path_keys = Enum.map(paths, fn path -> String.split(path, ".") end)
      response_keys = String.split(response_path, ".")

      # Transform values by calculating mean
      transformed_values =
        series[:values]
        |> Enum.map(fn value_map ->
          # Check if we can create the response path for this specific value_map
          if can_create_response_path?(value_map, response_keys) do
            scalar_values = Enum.map(path_keys, fn path_key -> get_path_value(value_map, path_key) end)

            case calculate_mean(scalar_values) do
              nil -> value_map
              mean_result -> put_path_value(value_map, response_keys, mean_result)
            end
          else
            # Skip this value_map if response path cannot be created
            value_map
          end
        end)

      %{series | values: transformed_values}
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

      value ->
        value
    end
  end

  # Helper function to put value at path - handles both string and atom keys
  defp put_path_value(value_map, keys, value) do
    # Try to determine if this map uses atom or string keys
    if map_uses_atom_keys?(value_map) do
      atom_keys = Enum.map(keys, &String.to_atom/1)
      put_in(value_map, atom_keys, value)
    else
      put_in(value_map, keys, value)
    end
  end

  # Determine if a map primarily uses atom keys (for backward compatibility)
  defp map_uses_atom_keys?(value_map) when is_map(value_map) do
    keys = Map.keys(value_map)
    atom_count = Enum.count(keys, &is_atom/1)
    string_count = Enum.count(keys, &is_binary/1)
    atom_count >= string_count
  end

  # Check if we can safely create the response path without crashing
  defp can_create_response_path?(value_map, response_keys) do
    can_create_path?(value_map, response_keys, map_uses_atom_keys?(value_map))
  end

  # Check if path can be created - only fails if intermediate paths exist but are not maps
  defp can_create_path?(_map, [], _atom_keys?), do: true
  defp can_create_path?(map, [_key], _atom_keys?), do: true  # Can always set final key
  defp can_create_path?(map, [key | rest], atom_keys?) do
    actual_key = if atom_keys?, do: String.to_atom(key), else: key
    
    case Map.get(map, actual_key) do
      nil -> false  # put_in/3 cannot create intermediate structures - this will fail
      nested_map when is_map(nested_map) -> can_create_path?(nested_map, rest, atom_keys?)
      _non_map -> false  # Intermediate path exists but is not a map
    end
  end

  # Calculate mean with precision handling
  defp calculate_mean(values) when is_list(values) do
    # Skip calculation if any value is nil (missing data)
    if Enum.any?(values, &is_nil/1) do
      nil
    else
      numeric_values = Enum.filter(values, &is_number/1)
      
      case numeric_values do
        [] -> nil
        _ -> 
          count = length(numeric_values)
          sum = Precision.sum(numeric_values)
          result = Precision.divide(sum, count)
          # Convert to appropriate type based on precision mode
          if Precision.enabled?(), do: result, else: Precision.to_float(result)
      end
    end
  end

  defp calculate_mean(_values) do
    # Return nil for invalid inputs (nil, not a list, etc.)
    nil
  end
end