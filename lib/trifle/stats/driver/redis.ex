defmodule Trifle.Stats.Driver.Redis do
  defstruct connection: nil, prefix: "trfl", separator: "::"

  def new(connection, prefix \\ "trfl", separator \\ "::") do
    %Trifle.Stats.Driver.Redis{
      connection: connection,
      prefix: prefix,
      separator: separator
    }
  end

  def inc(keys, values, driver) do
    data = Trifle.Stats.Packer.pack(values)

    Enum.map(keys, fn key ->
      pkey = Enum.join([driver.prefix] ++ [key], driver.separator)

      Enum.each(data, fn {k, c} ->
        Redix.command(driver.connection, ["HINCRBY", pkey, k, c])
      end)
    end)
  end

  def set(keys, values, driver) do
    data = Trifle.Stats.Packer.pack(values)
    payload = map_to_payload(data)

    Enum.map(keys, fn key ->
      pkey = Enum.join([driver.prefix] ++ [key], driver.separator)

      Redix.command(driver.connection, ["HMSET", pkey] ++ payload)
    end)
  end

  def get(keys, driver) do
    Enum.map(keys, fn key ->
      pkey = Enum.join([driver.prefix] ++ [key], driver.separator)

      with {:ok, payload} <- Redix.command(driver.connection, ["HGETALL", pkey]),
        data <- payload_to_map(payload) do
        Trifle.Stats.Packer.unpack(data)
      end
    end)
  end

  def payload_to_map(payload) do
    Enum.chunk_every(payload, 2)
    |> Enum.reduce(%{}, fn ([k, v], acc) -> Map.merge(acc, %{k => v}) end)
  end

  def map_to_payload(map) do
    Enum.reduce(map, [], fn({k, v}, acc) -> acc ++ [k, v] end)
  end
end
