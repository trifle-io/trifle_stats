defmodule Trifle.Stats.SeriesTest do
  use ExUnit.Case

  describe "series functionality" do
    test "series creation and basic operations work" do
      data = %{
        at: [~U[2025-08-17 10:00:00Z], ~U[2025-08-17 11:00:00Z], ~U[2025-08-17 12:00:00Z]],
        values: [%{count: 10}, %{count: 20}, %{count: 30}]
      }
      series = Trifle.Stats.Series.new(data)
      
      assert %Trifle.Stats.Series{} = series
      assert series.series.values == [%{count: 10}, %{count: 20}, %{count: 30}]
    end
    
    test "series aggregation methods work" do
      data = %{
        at: [~U[2025-08-17 10:00:00Z], ~U[2025-08-17 11:00:00Z], ~U[2025-08-17 12:00:00Z]],
        values: [%{count: 10}, %{count: 20}, %{count: 30}]
      }
      series = Trifle.Stats.Series.new(data)
      
      # Test new pipe-friendly API methods
      avg_result = Trifle.Stats.Series.aggregate_mean(series, "count")
      assert avg_result == 20.0
      
      # Test sum method  
      sum_result = Trifle.Stats.Series.aggregate_sum(series, "count")
      assert sum_result == 60
      
      # Test max method
      max_result = Trifle.Stats.Series.aggregate_max(series, "count")
      assert max_result == 30
      
      # Test min method
      min_result = Trifle.Stats.Series.aggregate_min(series, "count")
      assert min_result == 10
    end
    
    test "series formatting methods work" do
      timeline_data = %{
        at: [~U[2025-08-17 10:00:00Z], ~U[2025-08-17 11:00:00Z]],
        values: [%{count: 5}, %{count: 10}]
      }
      series = Trifle.Stats.Series.new(timeline_data)
      
      # Test new pipe-friendly formatting API
      timeline_result = Trifle.Stats.Series.format_timeline(series, "count")
      assert is_list(timeline_result)
      assert length(timeline_result) == 2
      
      category_data = %{
        at: [~U[2025-08-17 10:00:00Z], ~U[2025-08-17 11:00:00Z]],
        values: [%{"0" => 5, "1" => 10}, %{"0" => 8, "1" => 12}]
      }
      category_series = Trifle.Stats.Series.new(category_data)
      
      # Test category formatting
      category_result = Trifle.Stats.Series.format_category(category_series, "0")
      assert is_list(category_result)
      assert length(category_result) == 2
    end
  end
end