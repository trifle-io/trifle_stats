defmodule Trifle.Stats.Designator.Behaviour do
  @moduledoc """
  Behaviour for designator implementations.
  
  Designators classify numeric values into buckets/categories for data analysis.
  They provide a consistent interface for value categorization.
  """

  @doc """
  Classify a numeric value into a bucket representation.
  
  ## Parameters
  - `designator`: The designator instance
  - `value`: Numeric value to classify
  
  ## Returns
  String representation of the bucket/category the value falls into.
  """
  @callback designate(designator :: struct(), value :: number()) :: String.t()
end