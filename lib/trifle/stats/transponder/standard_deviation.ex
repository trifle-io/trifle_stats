defmodule Trifle.Stats.Transponder.StandardDeviation do
  @moduledoc """
  Standard Deviation transponder - calculates statistical standard deviation from sum, count, and square statistics.

  This transponder uses the computational formula for standard deviation to calculate the result from
  pre-aggregated statistics (sum, count, and sum of squares). This matches the Ruby trifle-stats
  implementation and is more efficient than calculating from raw values.

  Uses the formula: √((count × square - sum²) / (count × (count - 1)))

  ## Usage

      # Calculate standard deviation from aggregated statistics
      Trifle.Stats.Transponder.StandardDeviation.transform(series, "metrics.sum", "metrics.count", "metrics.square", "metrics.stddev")
      
  ## Input Format

  Expected data structure with aggregated statistics:
      %{
        at: [timestamp1, timestamp2, ...],
        values: [
          %{"metrics" => %{"sum" => 500, "count" => 10, "square" => 27500}},
          %{"metrics" => %{"sum" => 750, "count" => 15, "square" => 42000}},
          ...
        ]
      }

  ## Output Format

  Returns modified series with standard deviation added:
      %{
        at: [timestamp1, timestamp2, ...],
        values: [
          %{"metrics" => %{"sum" => 500, "count" => 10, "square" => 27500, "stddev" => 12.25}},
          %{"metrics" => %{"sum" => 750, "count" => 15, "square" => 42000, "stddev" => 15.81}},
          ...
        ]
      }
  """

  @behaviour Trifle.Stats.Transponder.Behaviour

  alias Trifle.Stats.Precision

  @impl true
  def transform(series, sum_path, count_path, square_path, response_path, _slices \\ 1) do
    if Enum.empty?(series[:at]) do
      series
    else
      sum_keys = String.split(sum_path, ".")
      count_keys = String.split(count_path, ".")
      square_keys = String.split(square_path, ".")
      response_keys = String.split(response_path, ".")

      # Transform values by calculating standard deviation from sum, count, and square
      transformed_values =
        series[:values]
        |> Enum.map(fn value_map ->
          sum_value = get_path_value(value_map, sum_keys)
          count_value = get_path_value(value_map, count_keys)
          square_value = get_path_value(value_map, square_keys)

          stddev =
            calculate_standard_deviation_from_statistics(sum_value, count_value, square_value)

          put_path_value(value_map, response_keys, stddev)
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

  # Calculate standard deviation from sum, count, and square statistics (matches Ruby implementation)
  defp calculate_standard_deviation_from_statistics(sum, count, square)
       when is_number(sum) and is_number(count) and is_number(square) and count > 1 do
    # Use computational formula: sqrt((count * square - sum^2) / (count * (count - 1)))
    # This matches the Ruby implementation exactly
    numerator =
      Precision.sub(
        Precision.mult(count, square),
        Precision.mult(sum, sum)
      )

    denominator = Precision.mult(count, count - 1)

    if Precision.to_float(denominator) > 0 do
      variance = Precision.divide(numerator, denominator)

      # Handle negative variance (can happen due to rounding errors)
      variance_float = Precision.to_float(variance)

      if variance_float < 0 do
        # Return 0 for negative variance (like Ruby's nan check)
        if Precision.enabled?(), do: Precision.to_decimal(0), else: 0.0
      else
        result = Precision.sqrt(variance)
        # Convert to appropriate type based on precision mode
        if Precision.enabled?(), do: result, else: Precision.to_float(result)
      end
    else
      # Return 0 for invalid denominator
      if Precision.enabled?(), do: Precision.to_decimal(0), else: 0.0
    end
  end

  # Handle Decimal values
  defp calculate_standard_deviation_from_statistics(%Decimal{} = sum, count, square)
       when is_number(count) and is_number(square) and count > 1 do
    calculate_standard_deviation_from_statistics(Precision.to_float(sum), count, square)
  end

  defp calculate_standard_deviation_from_statistics(sum, %Decimal{} = count, square)
       when is_number(sum) and is_number(square) do
    count_float = Precision.to_float(count)

    if count_float > 1 do
      calculate_standard_deviation_from_statistics(sum, count_float, square)
    else
      if Precision.enabled?(), do: Precision.to_decimal(0), else: 0.0
    end
  end

  defp calculate_standard_deviation_from_statistics(sum, count, %Decimal{} = square)
       when is_number(sum) and is_number(count) and count > 1 do
    calculate_standard_deviation_from_statistics(sum, count, Precision.to_float(square))
  end

  defp calculate_standard_deviation_from_statistics(_sum, _count, _square) do
    # Return 0 for invalid inputs (like Ruby's BigDecimal(0) for NaN)
    if Precision.enabled?(), do: Precision.to_decimal(0), else: 0.0
  end
end
