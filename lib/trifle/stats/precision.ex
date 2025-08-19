defmodule Trifle.Stats.Precision do
  @moduledoc """
  Precision handling utilities using the Decimal library for high-accuracy arithmetic.
  
  This module provides functions for converting between native numeric types and Decimal,
  performing high-precision calculations, and configuring precision levels throughout
  the Trifle.Stats library.
  
  ## Configuration
  
  Precision can be configured globally via application environment:
  
      config :trifle_stats, precision: [
        enabled: true,
        scale: 10,
        rounding: :half_up
      ]
  
  ## Usage Examples
  
      # Convert to decimal
      decimal = Trifle.Stats.Precision.to_decimal(42.5)
      
      # Perform precise division
      result = Trifle.Stats.Precision.divide(100, 3)
      
      # Calculate percentage with precision
      percentage = Trifle.Stats.Precision.percentage(sample, total)
      
      # Convert result back to float if needed
      float_result = Trifle.Stats.Precision.to_float(result)
  """
  
  @doc """
  Checks if precision mode is enabled.
  """
  def enabled? do
    Application.get_env(:trifle_stats, :precision, [])[:enabled] || false
  end
  
  @doc """
  Gets the configured decimal scale (number of decimal places).
  """
  def scale do
    Application.get_env(:trifle_stats, :precision, [])[:scale] || 10
  end
  
  @doc """
  Gets the configured rounding mode.
  """
  def rounding do
    Application.get_env(:trifle_stats, :precision, [])[:rounding] || :half_up
  end
  
  @doc """
  Converts a numeric value to Decimal with proper precision handling.
  """
  def to_decimal(value) when is_integer(value) do
    if enabled?() do
      Decimal.new(value)
      |> Decimal.round(scale(), rounding())
    else
      value
    end
  end
  
  def to_decimal(value) when is_float(value) do
    if enabled?() do
      # Convert float to string first, then to Decimal
      Decimal.new(Float.to_string(value))
      |> Decimal.round(scale(), rounding())
    else
      value
    end
  end
  
  def to_decimal(%Decimal{} = value) do
    if enabled?() do
      Decimal.round(value, scale(), rounding())
    else
      Decimal.to_float(value)
    end
  end
  
  def to_decimal(value), do: value
  
  @doc """
  Converts a value back to float, handling both Decimal and numeric types.
  """
  def to_float(%Decimal{} = decimal) do
    Decimal.to_float(decimal)
  end
  
  def to_float(value) when is_number(value), do: value * 1.0
  def to_float(_), do: 0.0
  
  @doc """
  Converts a value to the appropriate numeric type based on configuration.
  Returns Decimal if precision is enabled, otherwise returns float.
  """
  def to_numeric(value) when is_number(value) do
    if enabled?() do
      to_decimal(value)
    else
      value * 1.0
    end
  end
  
  def to_numeric(value), do: value
  
  @doc """
  Performs precise addition.
  """
  def add(a, b) when is_number(a) and is_number(b) do
    if enabled?() do
      Decimal.add(to_decimal(a), to_decimal(b))
    else
      a + b
    end
  end
  
  def add(%Decimal{} = a, b) when is_number(b) do
    Decimal.add(a, to_decimal(b))
  end
  
  def add(a, %Decimal{} = b) when is_number(a) do
    Decimal.add(to_decimal(a), b)
  end
  
  def add(%Decimal{} = a, %Decimal{} = b) do
    Decimal.add(a, b)
  end
  
  def add(a, b), do: a + b
  
  @doc """
  Performs precise subtraction.
  """
  def sub(a, b) when is_number(a) and is_number(b) do
    if enabled?() do
      Decimal.sub(to_decimal(a), to_decimal(b))
    else
      a - b
    end
  end
  
  def sub(%Decimal{} = a, b) when is_number(b) do
    Decimal.sub(a, to_decimal(b))
  end
  
  def sub(a, %Decimal{} = b) when is_number(a) do
    Decimal.sub(to_decimal(a), b)
  end
  
  def sub(%Decimal{} = a, %Decimal{} = b) do
    Decimal.sub(a, b)
  end
  
  def sub(a, b), do: a - b
  
  @doc """
  Performs precise multiplication.
  """
  def mult(a, b) when is_number(a) and is_number(b) do
    if enabled?() do
      Decimal.mult(to_decimal(a), to_decimal(b))
    else
      a * b
    end
  end
  
  def mult(%Decimal{} = a, b) when is_number(b) do
    Decimal.mult(a, to_decimal(b))
  end
  
  def mult(a, %Decimal{} = b) when is_number(a) do
    Decimal.mult(to_decimal(a), b)
  end
  
  def mult(%Decimal{} = a, %Decimal{} = b) do
    Decimal.mult(a, b)
  end
  
  def mult(a, b), do: a * b
  
  @doc """
  Performs precise division with safe zero handling.
  """
  def divide(a, b) when is_number(a) and is_number(b) and b != 0 do
    if enabled?() do
      Decimal.div(to_decimal(a), to_decimal(b))
    else
      a / b
    end
  end
  
  def divide(%Decimal{} = a, b) when is_number(b) and b != 0 do
    Decimal.div(a, to_decimal(b))
  end
  
  def divide(a, %Decimal{} = b) when is_number(a) do
    # Check if b is zero by converting to float and comparing
    if Decimal.to_float(b) != 0.0 do
      Decimal.div(to_decimal(a), b)
    else
      nil
    end
  end
  
  def divide(%Decimal{} = a, %Decimal{} = b) do
    if Decimal.to_float(b) != 0.0 do
      Decimal.div(a, b)
    else
      nil
    end
  end
  
  def divide(_a, 0), do: nil
  def divide(_a, _b), do: nil
  
  @doc """
  Calculates percentage as (value / total) * 100 with precision.
  """
  def percentage(value, total) when is_number(value) and is_number(total) and total > 0 do
    if enabled?() do
      value_decimal = to_decimal(value)
      total_decimal = to_decimal(total)
      hundred = to_decimal(100)
      
      Decimal.div(value_decimal, total_decimal)
      |> Decimal.mult(hundred)
    else
      (value / total) * 100.0
    end
  end
  
  # Handle Decimal inputs
  def percentage(%Decimal{} = value, total) when is_number(total) and total > 0 do
    if enabled?() do
      total_decimal = to_decimal(total)
      hundred = to_decimal(100)
      
      Decimal.div(value, total_decimal)
      |> Decimal.mult(hundred)
    else
      (Decimal.to_float(value) / total) * 100.0
    end
  end
  
  def percentage(value, %Decimal{} = total) when is_number(value) do
    total_float = Decimal.to_float(total)
    if total_float > 0 do
      if enabled?() do
        value_decimal = to_decimal(value)
        hundred = to_decimal(100)
        
        Decimal.div(value_decimal, total)
        |> Decimal.mult(hundred)
      else
        (value / total_float) * 100.0
      end
    else
      nil
    end
  end
  
  def percentage(%Decimal{} = value, %Decimal{} = total) do
    total_float = Decimal.to_float(total)
    if total_float > 0 do
      if enabled?() do
        hundred = to_decimal(100)
        
        Decimal.div(value, total)
        |> Decimal.mult(hundred)
      else
        (Decimal.to_float(value) / total_float) * 100.0
      end
    else
      nil
    end
  end
  
  def percentage(_value, _total), do: nil
  
  @doc """
  Calculates sum of a list with precision.
  """
  def sum(values) when is_list(values) do
    # Filter to include both numbers and Decimal values
    numeric_values = Enum.filter(values, fn x -> is_number(x) or match?(%Decimal{}, x) end)
    
    case numeric_values do
      [] -> 0
      _ ->
        if enabled?() do
          # Convert all to decimal and sum with Decimal precision
          numeric_values
          |> Enum.map(&to_decimal/1)
          |> Enum.reduce(Decimal.new(0), &add/2)
        else
          # Use regular sum for non-precision mode
          numeric_values
          |> Enum.filter(&is_number/1)  # Only numbers in non-precision mode
          |> Enum.sum()
        end
    end
  end
  
  def sum(_), do: 0
  
  @doc """
  Calculates average of a list with precision.
  """
  def average(values) when is_list(values) do
    numeric_values = Enum.filter(values, &is_number/1)
    
    case length(numeric_values) do
      0 -> nil
      count ->
        total = sum(numeric_values)
        # Handle case where sum might be 0 but count > 0
        case total do
          0 -> 0
          _ -> divide(total, count)
        end
    end
  end
  
  def average(_), do: nil
  
  @doc """
  Performs power operation with precision.
  """
  def pow(base, exponent) when is_number(base) and is_number(exponent) do
    if enabled?() do
      # Decimal doesn't have built-in power, so we use math and convert
      result = :math.pow(to_float(base), to_float(exponent))
      to_decimal(result)
    else
      :math.pow(base, exponent)
    end
  end
  
  def pow(%Decimal{} = base, exponent) when is_number(exponent) do
    result = :math.pow(Decimal.to_float(base), exponent)
    to_decimal(result)
  end
  
  def pow(base, %Decimal{} = exponent) when is_number(base) do
    result = :math.pow(base, Decimal.to_float(exponent))
    to_decimal(result)
  end
  
  def pow(%Decimal{} = base, %Decimal{} = exponent) do
    result = :math.pow(Decimal.to_float(base), Decimal.to_float(exponent))
    to_decimal(result)
  end
  
  def pow(_base, _exponent), do: nil
  
  @doc """
  Performs square root operation with precision.
  """
  def sqrt(value) when is_number(value) and value >= 0 do
    if enabled?() do
      # Decimal doesn't have built-in sqrt, so we use math and convert
      result = :math.sqrt(to_float(value))
      to_decimal(result)
    else
      :math.sqrt(value)
    end
  end
  
  def sqrt(%Decimal{} = value) do
    float_value = Decimal.to_float(value)
    if float_value >= 0 do
      result = :math.sqrt(float_value)
      to_decimal(result)
    else
      nil
    end
  end
  
  def sqrt(_value), do: nil
  
  @doc """
  Compares two numeric values, handling both Decimal and regular numbers.
  Returns -1, 0, or 1 for less than, equal to, or greater than.
  """
  def compare(a, b) when is_number(a) and is_number(b) do
    if enabled?() do
      case Decimal.compare(to_decimal(a), to_decimal(b)) do
        :lt -> -1
        :eq -> 0
        :gt -> 1
      end
    else
      cond do
        a < b -> -1
        a > b -> 1
        true -> 0
      end
    end
  end
  
  def compare(%Decimal{} = a, %Decimal{} = b) do
    case Decimal.compare(a, b) do
      :lt -> -1
      :eq -> 0
      :gt -> 1
    end
  end
  
  def compare(%Decimal{} = a, b) when is_number(b), do: compare(a, to_decimal(b))
  def compare(a, %Decimal{} = b) when is_number(a), do: compare(to_decimal(a), b)
  def compare(a, b) when a < b, do: -1
  def compare(a, b) when a > b, do: 1
  def compare(_a, _b), do: 0
  
  @doc """
  Finds maximum value in a list with precision handling.
  """
  def max(values) when is_list(values) do
    numeric_values = Enum.filter(values, &is_number/1)
    
    case numeric_values do
      [] -> nil
      [single] -> to_numeric(single)
      _ ->
        if enabled?() do
          numeric_values
          |> Enum.map(&to_decimal/1)
          |> Enum.max_by(&Decimal.to_float/1)
        else
          Enum.max(numeric_values)
        end
    end
  end
  
  def max(_), do: nil
  
  @doc """
  Finds minimum value in a list with precision handling.
  """
  def min(values) when is_list(values) do
    numeric_values = Enum.filter(values, &is_number/1)
    
    case numeric_values do
      [] -> nil
      [single] -> to_numeric(single)
      _ ->
        if enabled?() do
          numeric_values
          |> Enum.map(&to_decimal/1)
          |> Enum.min_by(&Decimal.to_float/1)
        else
          Enum.min(numeric_values)
        end
    end
  end
  
  def min(_), do: nil
end