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
  def transform(series, left, right, response_path, _slices \\ 1) do
    if Enum.empty?(series[:at]) do
      series
    else
      left_keys = String.split(left, ".")
      right_keys = String.split(right, ".")
      response_keys = String.split(response_path, ".")

      # Transform values by calculating ratios
      transformed_values =
        series[:values]
        |> Enum.map(fn value_map ->
          # Check if we can create the response path for this specific value_map
          if can_create_response_path?(value_map, response_keys) do
            left_value = get_path_value(value_map, left_keys)
            right_value = get_path_value(value_map, right_keys)

            ratio = calculate_ratio(left_value, right_value)
            put_path_value(value_map, response_keys, ratio)
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
  defp can_create_path?(_map, [_key], _atom_keys?), do: true
  defp can_create_path?(map, [key | rest], atom_keys?) do
    actual_key = if atom_keys?, do: String.to_atom(key), else: key
    
    case Map.get(map, actual_key) do
      nil -> false  # put_in/3 cannot create intermediate structures - this will fail
      nested_map when is_map(nested_map) -> can_create_path?(nested_map, rest, atom_keys?)
      _non_map -> false  # Intermediate path exists but is not a map
    end
  end

  # Calculate ratio as percentage with safe division and precision handling
  defp calculate_ratio(left, right) when is_number(left) and is_number(right) and right > 0 do
    result = Precision.percentage(left, right)
    # Convert to appropriate type based on precision mode
    if Precision.enabled?(), do: result, else: Precision.to_float(result)
  end

  # Handle Decimal left values
  defp calculate_ratio(%Decimal{} = left, right) when is_number(right) and right > 0 do
    result = Precision.percentage(left, right)
    if Precision.enabled?(), do: result, else: Precision.to_float(result)
  end

  # Handle Decimal right values
  defp calculate_ratio(left, %Decimal{} = right) when is_number(left) do
    right_float = Decimal.to_float(right)

    if right_float > 0 do
      result = Precision.percentage(left, right)
      if Precision.enabled?(), do: result, else: Precision.to_float(result)
    else
      nil
    end
  end

  # Handle both Decimal values
  defp calculate_ratio(%Decimal{} = left, %Decimal{} = right) do
    right_float = Decimal.to_float(right)

    if right_float > 0 do
      result = Precision.percentage(left, right)
      if Precision.enabled?(), do: result, else: Precision.to_float(result)
    else
      nil
    end
  end

  defp calculate_ratio(_left, _right) do
    # Return nil for invalid inputs (division by zero, nil values, etc.)
    nil
  end
end
