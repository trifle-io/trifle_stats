defmodule Trifle.Stats.PrecisionTest do
  use ExUnit.Case
  
  alias Trifle.Stats.Precision
  
  describe "precision configuration" do
    test "enabled? returns false by default" do
      refute Precision.enabled?()
    end
    
    test "scale returns default value of 10" do
      assert Precision.scale() == 10
    end
    
    test "rounding returns default value of :half_up" do
      assert Precision.rounding() == :half_up
    end
  end
  
  describe "conversion functions" do
    test "to_decimal converts numbers when precision is disabled" do
      # When precision is disabled, returns the original value
      assert Precision.to_decimal(42.5) == 42.5
      assert Precision.to_decimal(100) == 100
    end
    
    test "to_float converts various types to float" do
      assert Precision.to_float(42) == 42.0
      assert Precision.to_float(42.5) == 42.5
      assert Precision.to_float("invalid") == 0.0
    end
    
    test "to_numeric returns float when precision is disabled" do
      assert Precision.to_numeric(42) == 42.0
      assert Precision.to_numeric(42.5) == 42.5
    end
  end
  
  describe "arithmetic operations (precision disabled)" do
    test "add performs regular addition" do
      assert Precision.add(10, 20) == 30
      assert Precision.add(10.5, 20.7) == 31.2
    end
    
    test "sub performs regular subtraction" do
      assert Precision.sub(30, 10) == 20
      assert Precision.sub(30.5, 10.2) == 20.3
    end
    
    test "mult performs regular multiplication" do
      assert Precision.mult(10, 20) == 200
      assert Precision.mult(10.5, 2) == 21.0
    end
    
    test "divide performs safe division" do
      assert Precision.divide(100, 10) == 10.0
      assert Precision.divide(100.5, 2) == 50.25
      assert Precision.divide(100, 0) == nil
      assert Precision.divide(100, "invalid") == nil
    end
    
    test "percentage calculates percentages correctly" do
      assert Precision.percentage(25, 100) == 25.0
      # Use a less precise float comparison for division results
      result = Precision.percentage(1, 3)
      assert_in_delta result, 33.333333333333336, 0.0000000001
      assert Precision.percentage(100, 0) == nil
      assert Precision.percentage(100, "invalid") == nil
    end
  end
  
  describe "aggregate operations (precision disabled)" do
    test "sum calculates sum of list" do
      assert Precision.sum([1, 2, 3, 4, 5]) == 15
      assert Precision.sum([1.5, 2.5, 3.0]) == 7.0
      assert Precision.sum([1, "invalid", 3]) == 4
      assert Precision.sum([]) == 0
      assert Precision.sum("invalid") == 0
    end
    
    test "average calculates mean of list" do
      assert Precision.average([10, 20, 30]) == 20.0
      assert Precision.average([1, 2, 3, 4, 5]) == 3.0
      assert Precision.average([10.5, 20.5]) == 15.5
      assert Precision.average([]) == nil
      assert Precision.average("invalid") == nil
    end
    
    test "max finds maximum value" do
      assert Precision.max([1, 5, 3, 2]) == 5
      assert Precision.max([1.5, 5.7, 3.2]) == 5.7
      assert Precision.max([10]) == 10.0
      assert Precision.max([]) == nil
      assert Precision.max("invalid") == nil
    end
    
    test "min finds minimum value" do
      assert Precision.min([1, 5, 3, 2]) == 1
      assert Precision.min([1.5, 5.7, 3.2]) == 1.5
      assert Precision.min([10]) == 10.0
      assert Precision.min([]) == nil
      assert Precision.min("invalid") == nil
    end
  end
  
  describe "mathematical operations (precision disabled)" do
    test "pow performs power operation" do
      assert Precision.pow(2, 3) == 8.0
      assert Precision.pow(10, 2) == 100.0
      assert Precision.pow(2.5, 2) == 6.25
      assert Precision.pow("invalid", 2) == nil
    end
    
    test "sqrt performs square root operation" do
      assert Precision.sqrt(9) == 3.0
      assert Precision.sqrt(16) == 4.0
      assert Precision.sqrt(2.25) == 1.5
      assert Precision.sqrt(-1) == nil
      assert Precision.sqrt("invalid") == nil
    end
  end
  
  describe "comparison operations" do
    test "compare returns correct comparison values" do
      assert Precision.compare(10, 5) == 1
      assert Precision.compare(5, 10) == -1
      assert Precision.compare(10, 10) == 0
      assert Precision.compare(10.5, 10.5) == 0
    end
  end
  
  describe "precision enabled mode" do
    setup do
      # Store original config
      original_config = Application.get_env(:trifle_stats, :precision, [])
      
      # Set precision mode
      Application.put_env(:trifle_stats, :precision, [
        enabled: true,
        scale: 4,
        rounding: :half_up
      ])
      
      on_exit(fn ->
        # Restore original config
        if original_config == [] do
          Application.delete_env(:trifle_stats, :precision)
        else
          Application.put_env(:trifle_stats, :precision, original_config)
        end
      end)
      
      :ok
    end
    
    test "enabled? returns true when precision is configured" do
      assert Precision.enabled?()
    end
    
    test "scale returns configured value" do
      assert Precision.scale() == 4
    end
    
    test "to_decimal returns Decimal struct when precision is enabled" do
      result = Precision.to_decimal(42.123456)
      assert %Decimal{} = result
      assert Decimal.to_float(result) == 42.1235
    end
    
    test "to_numeric returns Decimal when precision is enabled" do
      result = Precision.to_numeric(42.5)
      assert %Decimal{} = result
    end
    
    test "arithmetic operations return Decimal when precision is enabled" do
      result = Precision.add(10.123456, 20.654321)
      assert %Decimal{} = result
      assert Decimal.to_float(result) == 30.7778
    end
    
    test "division maintains precision" do
      result = Precision.divide(1, 3)
      assert %Decimal{} = result
      # Should be rounded to 4 decimal places (0.3333)
      assert_in_delta Decimal.to_float(result), 0.3333, 0.0001
    end
    
    test "percentage calculation maintains precision" do
      result = Precision.percentage(1, 3)
      assert %Decimal{} = result
      # Should be 33.3333 with scale 4  
      assert_in_delta Decimal.to_float(result), 33.3333, 0.0001
    end
    
    test "sum returns Decimal when precision is enabled" do
      result = Precision.sum([1.111111, 2.222222, 3.333333])
      assert %Decimal{} = result
      # Should be rounded to 4 decimal places (6.6667)
      assert_in_delta Decimal.to_float(result), 6.6667, 0.0001
    end
    
    test "average returns Decimal when precision is enabled" do
      result = Precision.average([10, 20, 30])
      assert %Decimal{} = result
      assert Decimal.to_float(result) == 20.0
    end
    
    test "max/min return Decimal when precision is enabled" do
      max_result = Precision.max([1.111, 2.222, 3.333])
      min_result = Precision.min([1.111, 2.222, 3.333])
      
      assert %Decimal{} = max_result
      assert %Decimal{} = min_result
      assert Decimal.to_float(max_result) == 3.333
      assert Decimal.to_float(min_result) == 1.111
    end
    
    test "comparison works with Decimal values" do
      d1 = Precision.to_decimal(10.1234)
      d2 = Precision.to_decimal(10.1235)
      
      assert Precision.compare(d1, d2) == -1
      assert Precision.compare(d2, d1) == 1
      assert Precision.compare(d1, d1) == 0
    end
  end
  
  describe "integration with aggregators and transponders" do
    test "precision affects aggregator calculations" do
      # This will be tested by running aggregators with precision enabled/disabled
      # We'll verify this works when precision is integrated
      :ok
    end
  end
end