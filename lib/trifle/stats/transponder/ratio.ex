defmodule Trifle.Stats.Transponder.Ratio do
  @moduledoc """
  Ratio transponder - calculates percentage ratios from sample and total values.

  This transponder takes sample and total data from time series and computes
  the ratio as a percentage (sample/total * 100). Useful for conversion rates,
  success percentages, and other ratio-based metrics.

  ## Usage

      # Transform sample/total columns into percentage
      Trifle.Stats.Transponder.Ratio.transform(series, "conversions", "visits", "conversion_rate")
      
      # With multiple time slices
      Trifle.Stats.Transponder.Ratio.transform(series, "sample", "total", "percentage", 3)

  ## Input Format

  Expected data structure:
      %{
        at: [timestamp1, timestamp2, ...],
        values: [
          %{conversions: 25, visits: 100},
          %{conversions: 18, visits: 120},
          ...
        ]
      }

  ## Output Format

  Returns modified series with ratio values added as percentages:
      %{
        at: [timestamp1, timestamp2, ...], 
        values: [
          %{conversions: 25, visits: 100, conversion_rate: 25.0},
          %{conversions: 18, visits: 120, conversion_rate: 15.0},
          ...
        ]
      }
  """

  @behaviour Trifle.Stats.Transponder.Behaviour

  alias Trifle.Stats.Precision

  @impl true
  def transform(series, sample_path, total_path, response_path, _slices \\ 1) do
    if Enum.empty?(series[:at]) do
      series
    else
      sample_keys = String.split(sample_path, ".")
      total_keys = String.split(total_path, ".")
      response_keys = String.split(response_path, ".")

      # Transform values by calculating ratios
      transformed_values =
        series[:values]
        |> Enum.map(fn value_map ->
          sample_value = get_path_value(value_map, sample_keys)
          total_value = get_path_value(value_map, total_keys)

          ratio = calculate_ratio(sample_value, total_value)
          put_path_value(value_map, response_keys, ratio)
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

  # Calculate ratio as percentage with safe division and precision handling
  defp calculate_ratio(sample, total) when is_number(sample) and is_number(total) and total > 0 do
    result = Precision.percentage(sample, total)
    # Convert to appropriate type based on precision mode
    if Precision.enabled?(), do: result, else: Precision.to_float(result)
  end

  # Handle Decimal sample values
  defp calculate_ratio(%Decimal{} = sample, total) when is_number(total) and total > 0 do
    result = Precision.percentage(sample, total)
    if Precision.enabled?(), do: result, else: Precision.to_float(result)
  end

  # Handle Decimal total values
  defp calculate_ratio(sample, %Decimal{} = total) when is_number(sample) do
    total_float = Decimal.to_float(total)

    if total_float > 0 do
      result = Precision.percentage(sample, total)
      if Precision.enabled?(), do: result, else: Precision.to_float(result)
    else
      nil
    end
  end

  # Handle both Decimal values
  defp calculate_ratio(%Decimal{} = sample, %Decimal{} = total) do
    total_float = Decimal.to_float(total)

    if total_float > 0 do
      result = Precision.percentage(sample, total)
      if Precision.enabled?(), do: result, else: Precision.to_float(result)
    else
      nil
    end
  end

  defp calculate_ratio(_sample, _total) do
    # Return nil for invalid inputs (division by zero, nil values, etc.)
    nil
  end
end
