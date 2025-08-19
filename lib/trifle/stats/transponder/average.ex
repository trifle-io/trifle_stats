defmodule Trifle.Stats.Transponder.Average do
  @moduledoc """
  Average transponder - calculates running averages from sum and count values.

  This transponder takes sum and count data from time series and computes
  the average (sum/count) for each data point. Handles edge cases like
  division by zero and missing data gracefully.

  ## Usage

      # Transform sum/count columns into average
      Trifle.Stats.Transponder.Average.transform(series, "sum", "count", "average")
      
      # With multiple time slices
      Trifle.Stats.Transponder.Average.transform(series, "sum", "count", "average", 5)

  ## Input Format

  Expected data structure:
      %{
        at: [timestamp1, timestamp2, ...],
        values: [
          %{sum: 100, count: 10},
          %{sum: 200, count: 15},
          ...
        ]
      }

  ## Output Format

  Returns modified series with average values added:
      %{
        at: [timestamp1, timestamp2, ...],
        values: [
          %{sum: 100, count: 10, average: 10.0},
          %{sum: 200, count: 15, average: 13.33},
          ...
        ]
      }
  """

  @behaviour Trifle.Stats.Transponder.Behaviour

  alias Trifle.Stats.Precision

  @impl true
  def transform(series, sum_path, count_path, response_path, _slices \\ 1) do
    if Enum.empty?(series[:at]) do
      series
    else
      sum_keys = String.split(sum_path, ".")
      count_keys = String.split(count_path, ".")
      response_keys = String.split(response_path, ".")

      # Transform values by calculating averages
      transformed_values =
        series[:values]
        |> Enum.map(fn value_map ->
          sum_value = get_path_value(value_map, sum_keys)
          count_value = get_path_value(value_map, count_keys)

          average = calculate_average(sum_value, count_value)
          put_path_value(value_map, response_keys, average)
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

  # Calculate average with safe division and precision handling
  defp calculate_average(sum, count) when is_number(sum) and is_number(count) and count > 0 do
    result = Precision.divide(sum, count)
    # Convert to appropriate type based on precision mode
    if Precision.enabled?(), do: result, else: Precision.to_float(result)
  end

  # Handle Decimal sum values
  defp calculate_average(%Decimal{} = sum, count) when is_number(count) and count > 0 do
    result = Precision.divide(sum, count)
    if Precision.enabled?(), do: result, else: Precision.to_float(result)
  end

  # Handle Decimal count values
  defp calculate_average(sum, %Decimal{} = count) when is_number(sum) do
    count_float = Decimal.to_float(count)

    if count_float > 0 do
      result = Precision.divide(sum, count)
      if Precision.enabled?(), do: result, else: Precision.to_float(result)
    else
      nil
    end
  end

  # Handle both Decimal values
  defp calculate_average(%Decimal{} = sum, %Decimal{} = count) do
    count_float = Decimal.to_float(count)

    if count_float > 0 do
      result = Precision.divide(sum, count)
      if Precision.enabled?(), do: result, else: Precision.to_float(result)
    else
      nil
    end
  end

  defp calculate_average(_sum, _count) do
    # Return nil for invalid inputs (division by zero, nil values, etc.)
    nil
  end
end
