defmodule Trifle.Stats.AggregatorTest do
  use ExUnit.Case

  describe "aggregator functions" do
    test "mean aggregator works" do
      data = %{
        at: [~U[2025-08-17 10:00:00Z], ~U[2025-08-17 11:00:00Z], ~U[2025-08-17 12:00:00Z]],
        values: [%{count: 10}, %{count: 20}, %{count: 30}]
      }
      result = Trifle.Stats.Aggregator.Mean.aggregate(data, "count")
      assert result == 20.0
      
      # Test with slicing
      result_sliced = Trifle.Stats.Aggregator.Mean.aggregate(data, "count", 2)
      assert length(result_sliced) == 2
    end
    
    test "sum aggregator works" do
      data = %{
        at: [~U[2025-08-17 10:00:00Z], ~U[2025-08-17 11:00:00Z], ~U[2025-08-17 12:00:00Z]],
        values: [%{count: 10}, %{count: 20}, %{count: 30}]
      }
      result = Trifle.Stats.Aggregator.Sum.aggregate(data, "count")
      assert result == 60
      
      # Test with slicing
      result_sliced = Trifle.Stats.Aggregator.Sum.aggregate(data, "count", 2)
      assert length(result_sliced) == 2
    end
    
    test "max aggregator works" do
      data = %{
        at: [~U[2025-08-17 10:00:00Z], ~U[2025-08-17 11:00:00Z], ~U[2025-08-17 12:00:00Z]],
        values: [%{count: 10}, %{count: 30}, %{count: 20}]
      }
      result = Trifle.Stats.Aggregator.Max.aggregate(data, "count")
      assert result == 30
    end
    
    test "min aggregator works" do
      data = %{
        at: [~U[2025-08-17 10:00:00Z], ~U[2025-08-17 11:00:00Z], ~U[2025-08-17 12:00:00Z]],
        values: [%{count: 10}, %{count: 30}, %{count: 20}]
      }
      result = Trifle.Stats.Aggregator.Min.aggregate(data, "count")
      assert result == 10
    end
  end
end