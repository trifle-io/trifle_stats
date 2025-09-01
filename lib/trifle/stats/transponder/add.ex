defmodule Trifle.Stats.Transponder.Add do
  @moduledoc """
  Add transponder - adds two numeric values together.

  This transponder takes two numeric fields and computes their sum (left + right).
  Handles missing data gracefully by skipping calculations when either value is missing.

  ## Usage

      # Add two fields together
      Trifle.Stats.Transponder.Add.transform(series, "requests.value_a", "requests.value_b", "requests.total")

  ## Input Format

  Expected data structure:
      %{
        at: [timestamp1, timestamp2, ...],
        values: [
          %{"requests" => %{"value_a" => 10, "value_b" => 5}},
          %{"requests" => %{"value_a" => 15, "value_b" => 8}},
          ...
        ]
      }

  ## Output Format

  Returns modified series with sum values added:
      %{
        at: [timestamp1, timestamp2, ...],
        values: [
          %{"requests" => %{"value_a" => 10, "value_b" => 5, "total" => 15}},
          %{"requests" => %{"value_a" => 15, "value_b" => 8, "total" => 23}},
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

      # Transform values by calculating addition
      transformed_values =
        series[:values]
        |> Enum.map(fn value_map ->
          # Check if we can create the response path for this specific value_map
          if can_create_response_path?(value_map, response_keys) do
            left_value = get_path_value(value_map, left_keys)
            right_value = get_path_value(value_map, right_keys)

            add_result = calculate_add(left_value, right_value)
            put_path_value(value_map, response_keys, add_result)
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

  # Calculate addition with precision handling
  defp calculate_add(left, right) when is_number(left) and is_number(right) do
    result = Precision.add(left, right)
    # Convert to appropriate type based on precision mode
    if Precision.enabled?(), do: result, else: Precision.to_float(result)
  end

  # Handle Decimal values
  defp calculate_add(%Decimal{} = left, right) when is_number(right) do
    result = Precision.add(left, right)
    if Precision.enabled?(), do: result, else: Precision.to_float(result)
  end

  defp calculate_add(left, %Decimal{} = right) when is_number(left) do
    result = Precision.add(left, right)
    if Precision.enabled?(), do: result, else: Precision.to_float(result)
  end

  defp calculate_add(%Decimal{} = left, %Decimal{} = right) do
    result = Precision.add(left, right)
    if Precision.enabled?(), do: result, else: Precision.to_float(result)
  end

  defp calculate_add(_left, _right) do
    # Return nil for invalid inputs (nil values, etc.)
    nil
  end
end