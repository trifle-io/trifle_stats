defmodule Trifle.Stats.Transponder.Mean do
  @moduledoc """
  Mean transponder - calculates the arithmetic mean of array values.

  This transponder takes an array of values and computes their average.
  Only numeric values are included in the calculation, non-numeric values are ignored.

  ## Usage

      # Calculate mean of array values
      Trifle.Stats.Transponder.Mean.transform(series, "requests.items", "requests.average")

  ## Input Format

  Expected data structure:
      %{
        at: [timestamp1, timestamp2, ...],
        values: [
          %{"requests" => %{"items" => [10, 20, 30]}},
          %{"requests" => %{"items" => [15, 25, 35]}},
          ...
        ]
      }

  ## Output Format

  Returns modified series with mean values added:
      %{
        at: [timestamp1, timestamp2, ...],
        values: [
          %{"requests" => %{"items" => [10, 20, 30], "average" => 20.0}},
          %{"requests" => %{"items" => [15, 25, 35], "average" => 25.0}},
          ...
        ]
      }
  """

  @behaviour Trifle.Stats.Transponder.Behaviour

  alias Trifle.Stats.Precision

  @impl true
  def transform(series, values_path, response_path, _unused_param \\ nil, _slices \\ 1) do
    if Enum.empty?(series[:at]) do
      series
    else
      values_keys = String.split(values_path, ".")
      response_keys = String.split(response_path, ".")

      # Transform values by calculating mean
      transformed_values =
        series[:values]
        |> Enum.map(fn value_map ->
          array_values = get_path_value(value_map, values_keys)

          mean_result = calculate_mean(array_values)
          put_path_value(value_map, response_keys, mean_result)
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

  # Calculate mean with precision handling
  defp calculate_mean(values) when is_list(values) do
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

  defp calculate_mean(_values) do
    # Return nil for invalid inputs (nil, not a list, etc.)
    nil
  end
end