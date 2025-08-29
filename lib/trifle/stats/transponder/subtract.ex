defmodule Trifle.Stats.Transponder.Subtract do
  @moduledoc """
  Subtract transponder - subtracts one numeric value from another.

  This transponder takes two numeric fields and computes their difference (left - right).
  Handles missing data gracefully by skipping calculations when either value is missing.

  ## Usage

      # Subtract one field from another
      Trifle.Stats.Transponder.Subtract.transform(series, "requests.total", "requests.errors", "requests.successful")

  ## Input Format

  Expected data structure:
      %{
        at: [timestamp1, timestamp2, ...],
        values: [
          %{"requests" => %{"total" => 100, "errors" => 5}},
          %{"requests" => %{"total" => 150, "errors" => 8}},
          ...
        ]
      }

  ## Output Format

  Returns modified series with difference values added:
      %{
        at: [timestamp1, timestamp2, ...],
        values: [
          %{"requests" => %{"total" => 100, "errors" => 5, "successful" => 95}},
          %{"requests" => %{"total" => 150, "errors" => 8, "successful" => 142}},
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

      # Transform values by calculating subtraction
      transformed_values =
        series[:values]
        |> Enum.map(fn value_map ->
          left_value = get_path_value(value_map, left_keys)
          right_value = get_path_value(value_map, right_keys)

          subtract_result = calculate_subtract(left_value, right_value)
          put_path_value(value_map, response_keys, subtract_result)
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

  # Calculate subtraction with precision handling
  defp calculate_subtract(left, right) when is_number(left) and is_number(right) do
    result = Precision.sub(left, right)
    # Convert to appropriate type based on precision mode
    if Precision.enabled?(), do: result, else: Precision.to_float(result)
  end

  # Handle Decimal values
  defp calculate_subtract(%Decimal{} = left, right) when is_number(right) do
    result = Precision.sub(left, right)
    if Precision.enabled?(), do: result, else: Precision.to_float(result)
  end

  defp calculate_subtract(left, %Decimal{} = right) when is_number(left) do
    result = Precision.sub(left, right)
    if Precision.enabled?(), do: result, else: Precision.to_float(result)
  end

  defp calculate_subtract(%Decimal{} = left, %Decimal{} = right) do
    result = Precision.sub(left, right)
    if Precision.enabled?(), do: result, else: Precision.to_float(result)
  end

  defp calculate_subtract(_left, _right) do
    # Return nil for invalid inputs (nil values, etc.)
    nil
  end
end