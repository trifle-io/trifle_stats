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
      assert avg_result == [20.0]
      
      # Test sum method  
      sum_result = Trifle.Stats.Series.aggregate_sum(series, "count")
      assert sum_result == [60.0]
      
      # Test max method
      max_result = Trifle.Stats.Series.aggregate_max(series, "count")
      assert max_result == [30.0]
      
      # Test min method
      min_result = Trifle.Stats.Series.aggregate_min(series, "count")
      assert min_result == [10.0]
    end
    
    test "series formatting methods work" do
      timeline_data = %{
        at: [~U[2025-08-17 10:00:00Z], ~U[2025-08-17 11:00:00Z]],
        values: [%{count: 5}, %{count: 10}]
      }
      series = Trifle.Stats.Series.new(timeline_data)
      
      # Test new pipe-friendly formatting API
      timeline_result = Trifle.Stats.Series.format_timeline(series, "count")
      assert %{"count" => entries} = timeline_result
      assert length(entries) == 2
      
      category_data = %{
        at: [~U[2025-08-17 10:00:00Z], ~U[2025-08-17 11:00:00Z]],
        values: [%{"0" => 5, "1" => 10}, %{"0" => 8, "1" => 12}]
      }
      category_series = Trifle.Stats.Series.new(category_data)
      
      # Test category formatting
      category_result = Trifle.Stats.Series.format_category(category_series, "0")
      assert category_result == %{"0" => 13.0}
    end

    test "timeline formatting supports wildcards with nested value paths" do
      data = %{
        at: [~U[2025-08-17 10:00:00Z], ~U[2025-08-17 11:00:00Z]],
        values: [
          %{
            "base" => %{
              "offers" => %{
                "A10LE8TF2RTIHL" => %{"price" => 116},
                "A1DBBKW5BFCAVH" => %{"price" => 266}
              }
            }
          },
          %{
            "base" => %{
              "offers" => %{
                "A10LE8TF2RTIHL" => %{"price" => 117},
                "A1DBBKW5BFCAVH" => %{"price" => 250}
              }
            }
          }
        ]
      }

      series = Trifle.Stats.Series.new(data)
      timeline = Trifle.Stats.Series.format_timeline(series, "base.offers.*.price")

      assert Map.has_key?(timeline, "base.offers.A10LE8TF2RTIHL.price")
      assert Map.has_key?(timeline, "base.offers.A1DBBKW5BFCAVH.price")

      prices_a =
        timeline["base.offers.A10LE8TF2RTIHL.price"]
        |> Enum.map(& &1.value)

      prices_b =
        timeline["base.offers.A1DBBKW5BFCAVH.price"]
        |> Enum.map(& &1.value)

      assert prices_a == [116.0, 117.0]
      assert prices_b == [266.0, 250.0]
    end
  end
end
