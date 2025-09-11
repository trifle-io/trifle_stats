defmodule Trifle.Stats.PrecisionIntegrationTest do
  use ExUnit.Case
  
  alias Trifle.Stats.{Aggregator, Transponder}
  
  # Sample data with precision-sensitive calculations (using string keys)
  @sample_data %{
    at: [~U[2025-08-18 10:00:00Z], ~U[2025-08-18 11:00:00Z], ~U[2025-08-18 12:00:00Z]],
    values: [
      %{"price" => 10.333, "quantity" => 3, "sum" => 31.0, "count" => 3},
      %{"price" => 20.666, "quantity" => 7, "sum" => 62.0, "count" => 6}, 
      %{"price" => 15.123, "quantity" => 5, "sum" => 93.0, "count" => 9}
    ]
  }
  
  describe "precision disabled (default behavior)" do
    test "aggregators return float results" do
      # Test mean aggregator
      [avg_result] = Aggregator.Mean.aggregate(@sample_data, "price")
      assert is_float(avg_result)
      assert_in_delta avg_result, 15.374, 0.001
      
      # Test sum aggregator
      [sum_result] = Aggregator.Sum.aggregate(@sample_data, "quantity")
      assert sum_result == 15.0
      
      # Test max aggregator
      [max_result] = Aggregator.Max.aggregate(@sample_data, "price")
      assert is_float(max_result)
      assert max_result == 20.666
    end
    
    test "transponders return float results" do
      # Test divide transponder
      result = Transponder.Divide.transform(@sample_data, "sum", "count", "average")
      values = result.values
      
      first_avg = Enum.at(values, 0)["average"]
      assert is_float(first_avg)
      assert_in_delta first_avg, 10.333, 0.001
      
      # Test ratio transponder  
      ratio_result = Transponder.Ratio.transform(@sample_data, "sum", "count", "percentage")
      ratio_values = ratio_result.values
      
      first_ratio = Enum.at(ratio_values, 0)["percentage"]
      assert is_float(first_ratio)
      assert_in_delta first_ratio, 1033.333, 0.001
    end
  end
  
  describe "precision enabled mode" do
    setup do
      # Store original config
      original_config = Application.get_env(:trifle_stats, :precision, [])
      
      # Set precision mode with high precision
      Application.put_env(:trifle_stats, :precision, [
        enabled: true,
        scale: 6,
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
    
    test "aggregators return Decimal results when precision is enabled" do
      # Test mean aggregator with high precision
      [avg_result] = Aggregator.Mean.aggregate(@sample_data, "price")
      assert %Decimal{} = avg_result
      
      # Convert to float for comparison - should maintain higher precision
      float_result = Decimal.to_float(avg_result)
      assert_in_delta float_result, 15.374, 0.001
      
      # Test sum aggregator
      [sum_result] = Aggregator.Sum.aggregate(@sample_data, "quantity")
      assert %Decimal{} = sum_result
      assert Decimal.to_float(sum_result) == 15.0
      
      # Test max aggregator
      [max_result] = Aggregator.Max.aggregate(@sample_data, "price") 
      assert %Decimal{} = max_result
      assert Decimal.to_float(max_result) == 20.666
    end
    
    test "transponders return Decimal results when precision is enabled" do
      # Test divide transponder with precision
      result = Transponder.Divide.transform(@sample_data, "sum", "count", "average")
      values = result.values
      
      first_avg = Enum.at(values, 0)["average"]
      assert %Decimal{} = first_avg
      assert_in_delta Decimal.to_float(first_avg), 10.333333, 0.000001
      
      # Test ratio transponder with precision
      ratio_result = Transponder.Ratio.transform(@sample_data, "sum", "count", "percentage")
      ratio_values = ratio_result.values
      
      first_ratio = Enum.at(ratio_values, 0)["percentage"]
      assert %Decimal{} = first_ratio
      assert_in_delta Decimal.to_float(first_ratio), 1033.333333, 0.000001
    end
    
    test "standard deviation maintains precision" do
      # Use statistics from high-precision values [100.123456, 200.654321, 300.987654]
      data_with_stats = %{
        at: [~U[2025-08-18 10:00:00Z], ~U[2025-08-18 11:00:00Z]],
        values: [
          %{"sum" => 601.765431, "count" => 3, "square" => 141165.432},
          %{"sum" => 750.666666, "count" => 3, "square" => 187750.111}
        ]
      }
      
      result = Transponder.StandardDeviation.transform(data_with_stats, "sum", "count", "square", "stddev")
      values = result.values
      
      first_stddev = Enum.at(values, 0)["stddev"]
      assert %Decimal{} = first_stddev
      
      # Standard deviation should be calculated with high precision
      float_result = Decimal.to_float(first_stddev)
      assert float_result > 80.0 and float_result < 105.0
    end
    
    test "complex chained calculations maintain precision" do
      # Start with divide calculation
      avg_result = Transponder.Divide.transform(@sample_data, "sum", "count", "average")
      
      # Check that average was calculated correctly with precision
      first_avg_value = Enum.at(avg_result.values, 0)
      assert %Decimal{} = first_avg_value["average"]
      assert_in_delta Decimal.to_float(first_avg_value["average"]), 10.333333, 0.000001
      
      # Then calculate ratios on the newly calculated averages vs original count
      ratio_result = Transponder.Ratio.transform(avg_result, "average", "count", "efficiency")
      
      # All intermediate results should be Decimal
      values = ratio_result.values
      first_value = Enum.at(values, 0)
      
      assert %Decimal{} = first_value["average"]
      assert %Decimal{} = first_value["efficiency"]
      
      # Final efficiency calculation: (10.333333 / 3) * 100 = 344.444444
      efficiency = Decimal.to_float(first_value["efficiency"])
      assert_in_delta efficiency, 344.444444, 0.000001
    end
  end
  
  describe "precision mode functionality" do
    test "precision mode can handle large datasets without errors" do
      # Create a reasonably sized dataset to verify precision mode works 
      # without timing assertions that could be flaky
      large_dataset = %{
        at: Enum.map(1..100, fn i -> DateTime.add(~U[2025-01-01 00:00:00Z], i * 3600) end),
        values: Enum.map(1..100, fn i -> %{"value" => i * 1.123456789, "count" => i} end)
      }
      
      # Test with precision enabled - should not crash and should return Decimal
      Application.put_env(:trifle_stats, :precision, [enabled: true, scale: 4])
      
      [result] = Aggregator.Mean.aggregate(large_dataset, "value")
      
      # Clean up config
      Application.delete_env(:trifle_stats, :precision)
      
      # Should return a Decimal result and calculate correctly
      assert %Decimal{} = result
      expected_avg = (1..100) |> Enum.sum() |> Kernel.*(1.123456789) |> Kernel./(100)
      assert_in_delta Decimal.to_float(result), expected_avg, 0.01
    end
  end
  
  describe "mixed data types handling" do
    setup do
      Application.put_env(:trifle_stats, :precision, [enabled: true, scale: 4])
      
      on_exit(fn ->
        Application.delete_env(:trifle_stats, :precision)
      end)
      
      :ok
    end
    
    test "handles mixed integers and floats correctly" do
      mixed_data = %{
        at: [~U[2025-08-18 10:00:00Z], ~U[2025-08-18 11:00:00Z], ~U[2025-08-18 12:00:00Z]],
        values: [
          %{"amount" => 10},        # integer
          %{"amount" => 20.5},      # float  
          %{"amount" => 15}         # integer
        ]
      }
      
      [result] = Aggregator.Mean.aggregate(mixed_data, "amount")
      assert %Decimal{} = result
      assert_in_delta Decimal.to_float(result), 15.1667, 0.001
    end
    
    test "handles nil values gracefully" do
      data_with_nils = %{
        at: [~U[2025-08-18 10:00:00Z], ~U[2025-08-18 11:00:00Z], ~U[2025-08-18 12:00:00Z]],
        values: [
          %{"amount" => 10.5},
          %{"amount" => nil},       # nil value
          %{"amount" => 20.5}
        ]
      }
      
      [result] = Aggregator.Mean.aggregate(data_with_nils, "amount")
      assert %Decimal{} = result
      assert_in_delta Decimal.to_float(result), 15.5, 0.001
    end
  end
end
