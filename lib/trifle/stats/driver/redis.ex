defmodule Trifle.Stats.Driver.Redis do
  @moduledoc """
  Redis driver for Trifle.Stats with Ruby gem compatibility.

  Supports configurable key prefixes and full compatibility with Ruby trifle-stats.
  """

  defstruct connection: nil, prefix: "trfl", separator: "::"

  @doc """
  Create a new Redis driver instance.

  ## Parameters
  - `connection`: Redis connection
  - `prefix`: Key prefix for all stored keys (default: "trfl")
  - `separator`: Key separator (default: "::")

  ## Examples
      # Basic usage
      {:ok, conn} = Redix.start_link()
      driver = Trifle.Stats.Driver.Redis.new(conn)
      
      # With custom prefix (Ruby compatible)
      driver = Trifle.Stats.Driver.Redis.new(conn, "analytics")
  """
  def new(connection, prefix \\ "trfl", separator \\ "::") do
    %Trifle.Stats.Driver.Redis{
      connection: connection,
      prefix: prefix,
      separator: separator
    }
  end

  @doc """
  Create a new Redis driver from configuration (Ruby compatible).
  """
  def from_config(connection, %Trifle.Stats.Configuration{} = config) do
    prefix = Trifle.Stats.Configuration.driver_option(config, :prefix, "trfl")
    new(connection, prefix, config.separator)
  end

  def inc(keys, values, driver) do
    data = Trifle.Stats.Packer.pack(values)

    Enum.map(keys, fn %Trifle.Stats.Nocturnal.Key{} = key ->
      # Set prefix and join with separator
      prefixed_key = Trifle.Stats.Nocturnal.Key.set_prefix(key, driver.prefix)
      pkey = Trifle.Stats.Nocturnal.Key.join(prefixed_key, driver.separator)

      Enum.each(data, fn {k, c} ->
        Redix.command(driver.connection, ["HINCRBY", pkey, k, c])
      end)
    end)
  end

  def set(keys, values, driver) do
    # Use DELETE + HMSET to completely replace the hash like expected by tests
    Enum.map(keys, fn %Trifle.Stats.Nocturnal.Key{} = key ->
      # Set prefix and join with separator (exactly like Ruby)
      prefixed_key = Trifle.Stats.Nocturnal.Key.set_prefix(key, driver.prefix)
      pkey = Trifle.Stats.Nocturnal.Key.join(prefixed_key, driver.separator)

      # Delete existing hash first, then set new values to ensure complete replacement
      Redix.command(driver.connection, ["DEL", pkey])
      data = Trifle.Stats.Packer.pack(values)
      payload = map_to_payload(data)
      Redix.command(driver.connection, ["HMSET", pkey] ++ payload)
    end)
  end

  def get(keys, driver) do
    # Process each key in order and return results in same order (like Ruby)
    Enum.map(keys, fn %Trifle.Stats.Nocturnal.Key{} = key ->
      # Set prefix and join with separator (exactly like Ruby)
      prefixed_key = Trifle.Stats.Nocturnal.Key.set_prefix(key, driver.prefix)
      pkey = Trifle.Stats.Nocturnal.Key.join(prefixed_key, driver.separator)

      # Get hash data from Redis (like Ruby's hgetall)
      case Redix.command(driver.connection, ["HGETALL", pkey]) do
        {:ok, payload} ->
          data = payload_to_map(payload)
          Trifle.Stats.Packer.unpack(data)
        {:error, _} ->
          # Return empty hash on error (like Ruby behavior)
          %{}
      end
    end)
  end

  def payload_to_map(payload) do
    Enum.chunk_every(payload, 2)
    |> Enum.reduce(%{}, fn [k, v], acc -> Map.merge(acc, %{k => parse_value(v)}) end)
  end

  defp parse_value(value) do
    # Redis stores all values as strings, need to convert back to numbers
    case value do
      str when is_binary(str) ->
        # Try to parse as integer first
        case Integer.parse(str) do
          {int_val, ""} -> int_val
          {_int_val, _remainder} ->
            # Try to parse as float
            case Float.parse(str) do
              {float_val, ""} -> float_val
              {_float_val, _remainder} -> str  # Keep as string if not a clean number
            end
          :error -> str  # Keep as string if parsing fails
        end
      val -> val  # Return non-string values as-is
    end
  end

  def map_to_payload(map) do
    Enum.reduce(map, [], fn {k, v}, acc -> acc ++ [k, v] end)
  end

  def ping(%Trifle.Stats.Nocturnal.Key{} = _key, _values, _driver) do
    # Return :ok for successful ping operation like Ruby version (Redis doesn't support actual ping/scan operations)
    :ok
  end

  def scan(%Trifle.Stats.Nocturnal.Key{} = _key, _driver) do
    # Return empty array like Ruby version (Redis doesn't support ping/scan operations)
    []
  end
end
