defmodule Trifle.Stats.Designator.Custom do
  @moduledoc """
  Custom designator - classifies values using user-defined bucket boundaries.
  """
  
  @behaviour Trifle.Stats.Designator.Behaviour
  
  defstruct [:buckets]
  
  def new(buckets) when is_list(buckets) do
    %__MODULE__{buckets: Enum.sort(buckets)}
  end
  
  @impl true
  def designate(designator, value) when is_number(value) do
    buckets = designator.buckets
    
    cond do
      value <= hd(buckets) -> 
        "#{hd(buckets)}"
      value > List.last(buckets) -> 
        "#{List.last(buckets)}+"
      true -> 
        bucket = Enum.find(buckets, fn b -> ceil_value(value) < b end)
        "#{bucket}"
    end
  end
  
  defp ceil_value(value) when is_integer(value), do: value
  defp ceil_value(value) when is_float(value), do: :math.ceil(value) |> trunc()
end