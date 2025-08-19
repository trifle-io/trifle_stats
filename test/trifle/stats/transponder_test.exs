defmodule Trifle.Stats.TransponderTest do
  use ExUnit.Case

  describe "transponder functionality" do
    test "average transponder calculates correct averages" do
      data = %{
        at: [~U[2025-08-17 10:00:00Z], ~U[2025-08-17 11:00:00Z], ~U[2025-08-17 12:00:00Z]],
        values: [
          %{sum: 100, count: 10},
          %{sum: 200, count: 20},
          %{sum: 150, count: 15}
        ]
      }
      
      result = Trifle.Stats.Transponder.Average.transform(data, "sum", "count", "average")
      
      assert length(result.values) == 3
      [first, second, third] = result.values
      
      assert first.average == 10.0
      assert second.average == 10.0  
      assert third.average == 10.0
      
      # Original data should be preserved
      assert first.sum == 100
      assert first.count == 10
    end
    
    test "average transponder handles division by zero" do
      data = %{
        at: [~U[2025-08-17 10:00:00Z]],
        values: [%{sum: 100, count: 0}]
      }
      
      result = Trifle.Stats.Transponder.Average.transform(data, "sum", "count", "average")
      
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
      updated_series = Trifle.Stats.Series.transform_average(series, "sum", "count", "avg_value")
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
      assert {:module, Trifle.Stats.Transponder.Average} = Code.ensure_loaded(Trifle.Stats.Transponder.Average)
      assert {:module, Trifle.Stats.Transponder.Ratio} = Code.ensure_loaded(Trifle.Stats.Transponder.Ratio)
      assert {:module, Trifle.Stats.Transponder.StandardDeviation} = Code.ensure_loaded(Trifle.Stats.Transponder.StandardDeviation)
    end
    
    test "empty series handling works correctly" do
      empty_data = %{at: [], values: []}
      
      # All transponders should handle empty data gracefully
      avg_result = Trifle.Stats.Transponder.Average.transform(empty_data, "sum", "count", "average")
      ratio_result = Trifle.Stats.Transponder.Ratio.transform(empty_data, "sample", "total", "percentage")
      stddev_result = Trifle.Stats.Transponder.StandardDeviation.transform(empty_data, "sum", "count", "square", "stddev")
      
      assert avg_result == empty_data
      assert ratio_result == empty_data 
      assert stddev_result == empty_data
    end
  end
end