defmodule Trifle.Stats.Designator.Geometric do
  @moduledoc """
  Geometric designator - classifies values using logarithmic/geometric scaling with optional precision handling.
  Uses powers of 10 for bucket boundaries.
  
  Supports high-precision calculations using the Decimal library when precision mode
  is enabled via configuration.
  """
  
  @behaviour Trifle.Stats.Designator.Behaviour
  
  alias Trifle.Stats.Precision
  
  defstruct [:min, :max]
  
  def new(min, max) do
    adjusted_min = if min < 0, do: 0, else: min
    %__MODULE__{min: adjusted_min, max: max}
  end
  
  @impl true
  def designate(designator, value) when is_number(value) do
    min = designator.min
    max = designator.max
    
    cond do
      value <= min -> 
        "#{min |> to_float()}"
      value > max -> 
        "#{max |> to_float()}+"
      value > 1 -> 
        power = value |> floor_value() |> to_string() |> String.length()
        result = Precision.pow(10, power)
        "#{Precision.to_float(result)}"
      value > 0.1 -> 
        "1.0"
      true -> 
        # Handle small decimal values
        decimal_part = value |> to_string() |> String.replace("0.", "")
        leading_zeros = decimal_part |> String.split(~r/[1-9]/) |> hd() |> String.length()
        denominator = Precision.pow(10, leading_zeros)
        result = Precision.divide(1.0, denominator)
        "#{Precision.to_float(result)}"
    end
  end
  
  defp floor_value(value) when is_integer(value), do: value
  defp floor_value(value) when is_float(value), do: :math.floor(value) |> trunc()
  
  defp to_float(value) when is_integer(value), do: value * 1.0
  defp to_float(value) when is_float(value), do: value
end