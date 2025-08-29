defmodule Trifle.Stats.Transponder.Sum do
  @moduledoc """
  Sum transponder - calculates the sum of array values.

  This transponder takes an array of values and computes their total sum.
  Only numeric values are included in the calculation, non-numeric values are ignored.

  ## Usage

      # Sum array of values
      Trifle.Stats.Transponder.Sum.transform(series, "requests.items", "requests.total")

  ## Input Format

  Expected data structure:
      %{
        at: [timestamp1, timestamp2, ...],
        values: [
          %{"requests" => %{"items" => [10, 5, 15]}},
          %{"requests" => %{"items" => [20, 8, 12]}},
          ...
        ]
      }

  ## Output Format

  Returns modified series with sum values added:
      %{
        at: [timestamp1, timestamp2, ...],
        values: [
          %{"requests" => %{"items" => [10, 5, 15], "total" => 30}},
          %{"requests" => %{"items" => [20, 8, 12], "total" => 40}},
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

      # Transform values by calculating sum
      transformed_values =
        series[:values]
        |> Enum.map(fn value_map ->
          array_values = get_path_value(value_map, values_keys)

          sum_result = calculate_sum(array_values)
          put_path_value(value_map, response_keys, sum_result)
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

  # Calculate sum with precision handling
  defp calculate_sum(values) when is_list(values) do
    numeric_values = Enum.filter(values, &is_number/1)
    
    case numeric_values do
      [] -> nil
      _ -> 
        result = Precision.sum(numeric_values)
        if Precision.enabled?(), do: result, else: Precision.to_float(result)
    end
  end

  defp calculate_sum(_values) do
    # Return nil for invalid inputs (nil, not a list, etc.)
    nil
  end
end