defmodule Trifle.Stats.TransponderTest do
  use ExUnit.Case

  describe "transponder functionality" do
    test "add transponder calculates correct additions" do
      data = %{
        at: [~U[2025-08-17 10:00:00Z], ~U[2025-08-17 11:00:00Z], ~U[2025-08-17 12:00:00Z]],
        values: [
          %{value_a: 10, value_b: 5},
          %{value_a: 15, value_b: 8},
          %{value_a: 12, value_b: 3}
        ]
      }
      
      result = Trifle.Stats.Transponder.Add.transform(data, "value_a", "value_b", "total")
      
      assert length(result.values) == 3
      [first, second, third] = result.values
      
      assert first.total == 15.0
      assert second.total == 23.0  
      assert third.total == 15.0
      
      # Original data should be preserved
      assert first.value_a == 10
      assert first.value_b == 5
    end
    
    test "divide transponder calculates correct divisions" do
      data = %{
        at: [~U[2025-08-17 10:00:00Z], ~U[2025-08-17 11:00:00Z], ~U[2025-08-17 12:00:00Z]],
        values: [
          %{sum: 100, count: 10},
          %{sum: 200, count: 20},
          %{sum: 150, count: 15}
        ]
      }
      
      result = Trifle.Stats.Transponder.Divide.transform(data, "sum", "count", "average")
      
      assert length(result.values) == 3
      [first, second, third] = result.values
      
      assert first.average == 10.0
      assert second.average == 10.0  
      assert third.average == 10.0
      
      # Original data should be preserved
      assert first.sum == 100
      assert first.count == 10
    end
    
    test "divide transponder handles division by zero" do
      data = %{
        at: [~U[2025-08-17 10:00:00Z]],
        values: [%{sum: 100, count: 0}]
      }
      
      result = Trifle.Stats.Transponder.Divide.transform(data, "sum", "count", "average")
      
      [first] = result.values
      assert first.average == nil
    end
    
    test "ratio transponder calculates correct percentages" do
      data = %{
        at: [~U[2025-08-17 10:00:00Z], ~U[2025-08-17 11:00:00Z]],
        values: [
          %{conversions: 25, visits: 100},
          %{conversions: 30, visits: 200}
        ]
      }
      
      result = Trifle.Stats.Transponder.Ratio.transform(data, "conversions", "visits", "conversion_rate")
      
      assert length(result.values) == 2
      [first, second] = result.values
      
      assert first.conversion_rate == 25.0
      assert second.conversion_rate == 15.0
      
      # Original data should be preserved
      assert first.conversions == 25
      assert first.visits == 100
    end
    
    test "ratio transponder handles division by zero" do
      data = %{
        at: [~U[2025-08-17 10:00:00Z]],
        values: [%{conversions: 25, visits: 0}]
      }
      
      result = Trifle.Stats.Transponder.Ratio.transform(data, "conversions", "visits", "conversion_rate")
      
      [first] = result.values
      assert first.conversion_rate == nil
    end
    
    test "standard deviation transponder calculates correct values" do
      # Using sum, count, and square statistics approach like Ruby version
      data = %{
        at: [~U[2025-08-17 10:00:00Z], ~U[2025-08-17 11:00:00Z]],
        values: [
          # For values [100, 200, 300]: sum=600, count=3, square=140000
          %{sum: 600, count: 3, square: 140000},
          # For values [150, 150, 150]: sum=450, count=3, square=67500  
          %{sum: 450, count: 3, square: 67500}
        ]
      }
      
      result = Trifle.Stats.Transponder.StandardDeviation.transform(data, "sum", "count", "square", "stddev")
      
      assert length(result.values) == 2
      [first, second] = result.values
      
      # Check that standard deviation is calculated using computational formula
      # For [100,200,300]: sqrt((3*140000 - 600^2)/(3*2)) = sqrt(60000/6) = sqrt(10000) = 100
      assert abs(first.stddev - 100.0) < 0.01
      # For [150,150,150]: sqrt((3*67500 - 450^2)/(3*2)) = sqrt(0/6) = 0
      assert second.stddev == 0.0
      
      # Original data should be preserved
      assert first.sum == 600
      assert first.count == 3
    end
    
    test "standard deviation transponder handles count less than 2" do
      data = %{
        at: [~U[2025-08-17 10:00:00Z]],
        values: [%{sum: 150, count: 1, square: 22500}]
      }
      
      result = Trifle.Stats.Transponder.StandardDeviation.transform(data, "sum", "count", "square", "stddev")
      
      [first] = result.values
      assert first.stddev == 0.0
    end
    
    test "standard deviation transponder handles invalid data" do
      data = %{
        at: [~U[2025-08-17 10:00:00Z]],
        values: [%{sum: nil, count: 0, square: nil}]
      }
      
      result = Trifle.Stats.Transponder.StandardDeviation.transform(data, "sum", "count", "square", "stddev")
      
      [first] = result.values
      # Should return 0 for invalid inputs
      assert first.stddev == 0.0
    end
    
    test "sum transponder calculates correct sums from multiple paths" do
      data = %{
        at: [~U[2025-08-17 10:00:00Z], ~U[2025-08-17 11:00:00Z]],
        values: [
          %{"requests" => %{"items" => 10, "books" => 5, "shoes" => 15}},
          %{"requests" => %{"items" => 20, "books" => 8, "shoes" => 12}}
        ]
      }
      
      result = Trifle.Stats.Transponder.Sum.transform(data, ["requests.items", "requests.books", "requests.shoes"], "requests.total")
      
      assert length(result.values) == 2
      [first, second] = result.values
      
      assert first["requests"]["total"] == 30.0  # 10 + 5 + 15
      assert second["requests"]["total"] == 40.0  # 20 + 8 + 12
      
      # Original data should be preserved
      assert first["requests"]["items"] == 10
      assert first["requests"]["books"] == 5
    end
    
    test "min transponder finds correct minimums from multiple paths" do
      data = %{
        at: [~U[2025-08-17 10:00:00Z], ~U[2025-08-17 11:00:00Z]],
        values: [
          %{"requests" => %{"items" => 10, "books" => 5, "shoes" => 15}},
          %{"requests" => %{"items" => 20, "books" => 8, "shoes" => 12}}
        ]
      }
      
      result = Trifle.Stats.Transponder.Min.transform(data, ["requests.items", "requests.books", "requests.shoes"], "requests.minimum")
      
      assert length(result.values) == 2
      [first, second] = result.values
      
      assert first["requests"]["minimum"] == 5   # min(10, 5, 15)
      assert second["requests"]["minimum"] == 8  # min(20, 8, 12)
      
      # Original data should be preserved
      assert first["requests"]["items"] == 10
      assert first["requests"]["books"] == 5
    end
    
    test "max transponder finds correct maximums from multiple paths" do
      data = %{
        at: [~U[2025-08-17 10:00:00Z], ~U[2025-08-17 11:00:00Z]],
        values: [
          %{"requests" => %{"items" => 10, "books" => 5, "shoes" => 15}},
          %{"requests" => %{"items" => 20, "books" => 8, "shoes" => 12}}
        ]
      }
      
      result = Trifle.Stats.Transponder.Max.transform(data, ["requests.items", "requests.books", "requests.shoes"], "requests.maximum")
      
      assert length(result.values) == 2
      [first, second] = result.values
      
      assert first["requests"]["maximum"] == 15  # max(10, 5, 15)
      assert second["requests"]["maximum"] == 20  # max(20, 8, 12)
      
      # Original data should be preserved
      assert first["requests"]["items"] == 10
      assert first["requests"]["books"] == 5
    end
    
    test "mean transponder calculates correct means from multiple paths" do
      data = %{
        at: [~U[2025-08-17 10:00:00Z], ~U[2025-08-17 11:00:00Z]],
        values: [
          %{"requests" => %{"items" => 10, "books" => 5, "shoes" => 15}},
          %{"requests" => %{"items" => 6, "books" => 15, "shoes" => 9}}
        ]
      }
      
      result = Trifle.Stats.Transponder.Mean.transform(data, ["requests.items", "requests.books", "requests.shoes"], "requests.average")
      
      assert length(result.values) == 2
      [first, second] = result.values
      
      assert first["requests"]["average"] == 10.0  # (10 + 5 + 15) / 3
      assert second["requests"]["average"] == 10.0  # (6 + 15 + 9) / 3
      
      # Original data should be preserved
      assert first["requests"]["items"] == 10
      assert first["requests"]["books"] == 5
    end
    
    test "transponders handle missing data correctly" do
      data = %{
        at: [~U[2025-08-17 10:00:00Z]],
        values: [
          %{"requests" => %{"items" => 10}} # Missing 'books' path
        ]
      }
      
      # All transponders should skip calculation when any path is missing
      sum_result = Trifle.Stats.Transponder.Sum.transform(data, ["requests.items", "requests.books"], "requests.sum")
      min_result = Trifle.Stats.Transponder.Min.transform(data, ["requests.items", "requests.books"], "requests.min")
      max_result = Trifle.Stats.Transponder.Max.transform(data, ["requests.items", "requests.books"], "requests.max")
      mean_result = Trifle.Stats.Transponder.Mean.transform(data, ["requests.items", "requests.books"], "requests.mean")
      
      [sum_first] = sum_result.values
      [min_first] = min_result.values
      [max_first] = max_result.values
      [mean_first] = mean_result.values
      
      assert !Map.has_key?(sum_first["requests"], "sum")
      assert !Map.has_key?(min_first["requests"], "min")
      assert !Map.has_key?(max_first["requests"], "max")
      assert !Map.has_key?(mean_first["requests"], "mean")
      
      # Original data should be preserved
      assert sum_first["requests"]["items"] == 10
      assert min_first["requests"]["items"] == 10
      assert max_first["requests"]["items"] == 10
      assert mean_first["requests"]["items"] == 10
    end
    
    test "series integration with transponders works" do
      # Create series with sum/count data
      data = %{
        at: [~U[2025-08-17 10:00:00Z], ~U[2025-08-17 11:00:00Z]],
        values: [
          %{sum: 100, count: 10, conversions: 5, visits: 50},
          %{sum: 200, count: 20, conversions: 8, visits: 40}
        ]
      }
      
      series = Trifle.Stats.Series.new(data)
      
      # Test new pipe-friendly API with separate arguments
      updated_series = Trifle.Stats.Series.transform_divide(series, "sum", "count", "avg_value")
      assert %Trifle.Stats.Series{} = updated_series
      
      # Test chained transformations
      final_series = series
      |> Trifle.Stats.Series.transform_ratio("conversions", "visits", "conversion_rate")
      
      assert %Trifle.Stats.Series{} = final_series
      
      # Verify the transformations worked
      [first, second] = final_series.series.values
      assert first[:conversion_rate] == 10.0  # 5/50 * 100
      assert second[:conversion_rate] == 20.0  # 8/40 * 100
    end
    
    test "transponder modules are properly loaded" do
      # Test that all transponder modules compile and load
      assert {:module, Trifle.Stats.Transponder.Behaviour} = Code.ensure_loaded(Trifle.Stats.Transponder.Behaviour)
      assert {:module, Trifle.Stats.Transponder.Add} = Code.ensure_loaded(Trifle.Stats.Transponder.Add)
      assert {:module, Trifle.Stats.Transponder.Divide} = Code.ensure_loaded(Trifle.Stats.Transponder.Divide)
      assert {:module, Trifle.Stats.Transponder.Max} = Code.ensure_loaded(Trifle.Stats.Transponder.Max)
      assert {:module, Trifle.Stats.Transponder.Mean} = Code.ensure_loaded(Trifle.Stats.Transponder.Mean)
      assert {:module, Trifle.Stats.Transponder.Min} = Code.ensure_loaded(Trifle.Stats.Transponder.Min)
      assert {:module, Trifle.Stats.Transponder.Multiply} = Code.ensure_loaded(Trifle.Stats.Transponder.Multiply)
      assert {:module, Trifle.Stats.Transponder.Ratio} = Code.ensure_loaded(Trifle.Stats.Transponder.Ratio)
      assert {:module, Trifle.Stats.Transponder.StandardDeviation} = Code.ensure_loaded(Trifle.Stats.Transponder.StandardDeviation)
      assert {:module, Trifle.Stats.Transponder.Subtract} = Code.ensure_loaded(Trifle.Stats.Transponder.Subtract)
      assert {:module, Trifle.Stats.Transponder.Sum} = Code.ensure_loaded(Trifle.Stats.Transponder.Sum)
    end
    
    test "empty series handling works correctly" do
      empty_data = %{at: [], values: []}
      
      # All transponders should handle empty data gracefully
      add_result = Trifle.Stats.Transponder.Add.transform(empty_data, "left", "right", "result")
      divide_result = Trifle.Stats.Transponder.Divide.transform(empty_data, "sum", "count", "average")
      ratio_result = Trifle.Stats.Transponder.Ratio.transform(empty_data, "sample", "total", "percentage")
      stddev_result = Trifle.Stats.Transponder.StandardDeviation.transform(empty_data, "sum", "count", "square", "stddev")
      sum_result = Trifle.Stats.Transponder.Sum.transform(empty_data, ["items"], "total")
      min_result = Trifle.Stats.Transponder.Min.transform(empty_data, ["items"], "minimum")
      max_result = Trifle.Stats.Transponder.Max.transform(empty_data, ["items"], "maximum")
      mean_result = Trifle.Stats.Transponder.Mean.transform(empty_data, ["items"], "average")
      
      assert add_result == empty_data
      assert divide_result == empty_data
      assert ratio_result == empty_data 
      assert stddev_result == empty_data
      assert sum_result == empty_data
      assert min_result == empty_data
      assert max_result == empty_data
      assert mean_result == empty_data
    end
  end
end