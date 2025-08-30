defmodule Trifle.Stats.Packer do
  def pack(hash, prefix \\ nil) do
    Enum.reduce(hash, %{}, fn({k, v}, acc) ->
      key = [prefix, k] |> Enum.reject(&is_nil/1) |> Enum.join(".")
      cond do
        is_map(v) and not is_struct(v) ->
          Map.merge(acc, pack(v, key))
        true ->
          # Convert DateTime and other structs to appropriate values
          value = normalize_value(v)
          Map.merge(acc, %{key => value})
      end
    end)
  end
  
  # Helper to normalize values for storage
  defp normalize_value(%DateTime{} = dt), do: DateTime.to_unix(dt)
  defp normalize_value(v), do: v

  def unpack(hash) do
    Enum.reduce(hash, %{}, fn({key, v}, acc) ->
      deep_merge(
        acc,
        String.split(key, ".") |> Enum.reverse |> Enum.reduce(v, fn k, a -> %{k => a} end)
      )
    end)
  end

  def deep_merge(this_hash, other_hash) do
    Map.merge(this_hash, other_hash, &deep_merge!/3)
  end

  defp deep_merge!(_key, this_hash = %{}, other_hash = %{}) do
    deep_merge(this_hash, other_hash)
  end

  defp deep_merge!(_key, _this_hash, other_hash) do
    other_hash
  end

  def deep_sum(this_hash, other_hash) when is_nil(other_hash) do
    this_hash
  end

  def deep_sum(this_hash, other_hash) do
    Map.merge(this_hash, other_hash, &deep_sum!/3)
  end

  defp deep_sum!(_key, this_hash = %{}, other_hash = %{}) do
    deep_sum(this_hash, other_hash)
  end

  defp deep_sum!(_key, this_hash, other_hash) do
    this_hash + other_hash
  end

  def normalize(object) when is_list(object) do
    Enum.map(object, &normalize/1)
  end

  def normalize(%Decimal{} = decimal) do
    decimal
  end

  def normalize(object) when is_map(object) and not is_struct(object) do
    Map.new(object, fn {k, v} -> {k, normalize(v)} end)
  end

  def normalize(object) when is_number(object) do
    # Convert to Decimal for precision, or keep as is for simplicity
    object
  end

  def normalize(object), do: object
end
