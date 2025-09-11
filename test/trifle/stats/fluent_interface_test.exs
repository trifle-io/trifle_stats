defmodule Trifle.Stats.FluentInterfaceTest do
  use ExUnit.Case
  
  # Note: Fluent interface is now part of main Series module
  
  # Test sample data
  @sample_series %{
    at: [~U[2025-08-18 10:00:00Z], ~U[2025-08-18 11:00:00Z], ~U[2025-08-18 12:00:00Z]],
    values: [%{"count" => 10}, %{"count" => 20}, %{"count" => 30}]
  }
  
  @transponder_series %{
    at: [~U[2025-08-18 10:00:00Z], ~U[2025-08-18 11:00:00Z]],
    values: [%{"sum" => 100, "count" => 10}, %{"sum" => 200, "count" => 20}]
  }
  
  setup do
    series = Trifle.Stats.Series.new(@sample_series)
    transponder_series = Trifle.Stats.Series.new(@transponder_series)
    
    %{series: series, transponder_series: transponder_series}
  end
  
  describe "aggregator methods (terminal operations)" do
    test "aggregate_sum returns list with sum", %{series: series} do
      result = series |> Trifle.Stats.Series.aggregate_sum("count")
      assert result == [60.0]  # 10 + 20 + 30
    end
    
    test "aggregate_mean returns list with average", %{series: series} do
      result = series |> Trifle.Stats.Series.aggregate_mean("count")
      assert result == [20.0]  # (10 + 20 + 30) / 3
    end
    
    test "aggregate_max returns list with max value", %{series: series} do
      result = series |> Trifle.Stats.Series.aggregate_max("count")
      assert result == [30.0]
    end
    
    test "aggregate_min returns list with min value", %{series: series} do
      result = series |> Trifle.Stats.Series.aggregate_min("count")
      assert result == [10.0]
    end
    
    test "aggregator methods support slicing", %{series: series} do
      result = series |> Trifle.Stats.Series.aggregate_sum("count", 2)
      assert is_list(result)
      assert length(result) == 2
    end
    
    test "can chain aggregation with other pipe operations", %{series: series} do
      result = series
      |> Trifle.Stats.Series.aggregate_sum("count")
      |> then(&(hd(&1) * 2))  # multiply by 2
      |> then(&(&1 + 10)) # add 10
      
      assert result == 130  # (60 * 2) + 10
    end
  end
  
  describe "formatter methods (terminal operations)" do
    test "format_timeline returns formatted timeline data", %{series: series} do
      result = series |> Trifle.Stats.Series.format_timeline("count")
      
      # Timeline formatter returns list of maps with at/value keys
      assert is_list(result)
      assert length(result) == 3
      assert Enum.at(result, 0).value == 10.0
      assert Enum.at(result, 1).value == 20.0
      assert Enum.at(result, 2).value == 30.0
    end
    
    test "format_category returns formatted category data", %{series: series} do
      result = series |> Trifle.Stats.Series.format_category("count")
      
      # Category formatter returns list of category entries
      assert is_list(result)
      assert length(result) == 3
    end
    
    test "formatter methods support transform functions", %{series: series} do
      transform_fn = fn at, value -> %{at: at, value: value * 10} end
      result = series |> Trifle.Stats.Series.format_timeline("count", 1, transform_fn)
      
      assert Enum.at(result, 0).value == 100.0  # 10 * 10
      assert Enum.at(result, 1).value == 200.0  # 20 * 10
      assert Enum.at(result, 2).value == 300.0  # 30 * 10
    end
    
    test "can chain formatting with other pipe operations", %{series: series} do
      result = series
      |> Trifle.Stats.Series.format_timeline("count")
      |> length()
      
      assert result == 3
    end
  end
  
  describe "transformation methods (intermediate operations)" do
    test "transform_divide returns new Series", %{transponder_series: series} do
      result = series |> Trifle.Stats.Series.transform_divide("sum", "count", "avg")
      
      # Should return a Series struct
      assert %Trifle.Stats.Series{} = result
      
      # Check that division was calculated correctly
      values = result.series[:values]
      assert Enum.at(values, 0)["avg"] == 10.0  # 100/10
      assert Enum.at(values, 1)["avg"] == 10.0  # 200/20
    end
    
    test "transform_ratio returns new Series", %{transponder_series: series} do
      result = series |> Trifle.Stats.Series.transform_ratio("sum", "count", "ratio")
      
      assert %Trifle.Stats.Series{} = result
      
      # Check that ratio was calculated correctly (as percentage)
      values = result.series[:values]
      assert Enum.at(values, 0)["ratio"] == 1000.0  # (100/10) * 100 = 1000%
      assert Enum.at(values, 1)["ratio"] == 1000.0  # (200/20) * 100 = 1000%
    end
    
    test "can chain transformation operations", %{transponder_series: series} do
      result = series
      |> Trifle.Stats.Series.transform_divide("sum", "count", "avg")
      |> Trifle.Stats.Series.transform_ratio("avg", "count", "normalized")
      
      assert %Trifle.Stats.Series{} = result
      
      # Should have both avg and normalized fields
      values = result.series[:values]
      first_value = Enum.at(values, 0)
      assert Map.has_key?(first_value, "avg")
      assert Map.has_key?(first_value, "normalized")
    end
    
    test "can chain transformation to aggregator", %{transponder_series: series} do
      result = series
      |> Trifle.Stats.Series.transform_divide("sum", "count", "avg")  # intermediate -> Series
      |> Trifle.Stats.Series.aggregate_max("avg")                      # terminal -> raw data
      
      assert result == [10.0]  # max of the averages
    end
    
    test "can chain transformation to formatter", %{transponder_series: series} do
      result = series
      |> Trifle.Stats.Series.transform_divide("sum", "count", "avg")  # intermediate -> Series
      |> Trifle.Stats.Series.format_timeline("avg")                    # terminal -> formatted data
      
      assert is_list(result)
      assert length(result) == 2
      assert Enum.at(result, 0).value == 10.0
    end
  end
  
  # NOTE: Pipe operator integration removed - now using direct methods
  
  # NOTE: Convenience methods removed - using direct pipe-friendly API instead
  
  describe "backwards compatibility - deprecated" do
    # NOTE: Legacy proxy interface has been removed in favor of pipe-friendly design
    # Tests kept for reference but marked as pending
    
    @tag :skip
    test "legacy proxy interface (removed)", %{series: series} do
      # This API pattern has been deprecated
    end
  end
  
  describe "error handling" do
    test "methods validate series struct", %{series: series} do
      # Should work with proper Series struct
      assert series |> Trifle.Stats.Series.aggregate_sum("count") == [60.0]
      
      # Should raise with invalid input (expects KeyError for missing :series key)
      assert_raise KeyError, fn ->
        %{invalid: "data"} |> Trifle.Stats.Series.aggregate_sum("count")
      end
    end
    
    test "methods handle missing paths gracefully", %{series: series} do
      # Missing path should return 0 or nil for aggregators
      result = series |> Trifle.Stats.Series.aggregate_sum("missing_field")
      assert result == [0.0]
    end
  end
  
  describe "pipe-friendly chaining examples" do
    test "complex processing pipeline", %{transponder_series: series} do
      # Demonstrate clean pipe-friendly API
      result = series
      |> Trifle.Stats.Series.transform_divide("sum", "count", "avg")
      |> Trifle.Stats.Series.transform_ratio("sum", "count", "efficiency") 
      |> Trifle.Stats.Series.aggregate_max("efficiency")
      
      assert is_number(hd(result))
    end
    
    test "formatting after transformation", %{transponder_series: series} do
      result = series
      |> Trifle.Stats.Series.transform_divide("sum", "count", "avg")
      |> Trifle.Stats.Series.format_timeline("avg")
      |> length()
      
      assert result == 2
    end
  end
end
