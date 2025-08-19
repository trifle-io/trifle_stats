defmodule Trifle.Stats.Designator.Linear do
  @moduledoc """
  Linear designator - classifies values using fixed-step linear progression.
  """
  
  @behaviour Trifle.Stats.Designator.Behaviour
  
  defstruct [:min, :max, :step]
  
  def new(min, max, step) when is_number(step) and step > 0 do
    %__MODULE__{min: min, max: max, step: trunc(step)}
  end
  
  @impl true
  def designate(designator, value) when is_number(value) do
    min = designator.min
    max = designator.max
    step = designator.step
    
    cond do
      value <= min -> 
        "#{min}"
      value > max -> 
        "#{max}+"
      true -> 
        # Calculate the bucket using ceiling division and step rounding
        ceiled_value = ceil_value(value)
        bucket_multiplier = div(ceiled_value, step)
        remainder = rem(ceiled_value, step)
        
        bucket = bucket_multiplier * step + if remainder == 0, do: 0, else: step
        "#{bucket}"
    end
  end
  
  defp ceil_value(value) when is_integer(value), do: value
  defp ceil_value(value) when is_float(value), do: :math.ceil(value) |> trunc()
end