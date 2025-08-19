defmodule Trifle.Stats.SkipBlanksTest do
  use ExUnit.Case
  
  setup do
    {:ok, pid} = Trifle.Stats.Driver.Process.start_link()
    driver = Trifle.Stats.Driver.Process.new(pid)
    
    config = Trifle.Stats.Configuration.configure(
      driver, 
      time_zone: "UTC",
      time_zone_database: Tzdata.TimeZoneDatabase,
      track_granularities: [:hour, :day]
    )
    
    %{config: config}
  end
  
  describe "skip_blanks parameter" do
    test "values without skip_blanks includes empty data points", %{config: config} do
      # Create a timeline with some data points and some empty ones
      base_time = ~U[2025-08-18 10:00:00Z]
      
      # Track data only on specific hours
      Trifle.Stats.track("page_views", base_time, %{"count" => 5}, config)
      Trifle.Stats.track("page_views", DateTime.add(base_time, 2, :hour), %{"count" => 3}, config)
      
      # Query over a 4-hour granularity (10:00 - 13:00)
      result = Trifle.Stats.values("page_views", base_time, DateTime.add(base_time, 3, :hour), :hour, config)
      
      # Should return 4 time points with some empty values
      assert length(result.at) == 4
      assert length(result.values) == 4
      
      # First hour should have data
      assert result.values |> Enum.at(0) |> map_size() > 0
      # Second hour should be empty
      assert result.values |> Enum.at(1) |> map_size() == 0
      # Third hour should have data  
      assert result.values |> Enum.at(2) |> map_size() > 0
      # Fourth hour should be empty
      assert result.values |> Enum.at(3) |> map_size() == 0
    end
    
    test "values with skip_blanks filters out empty data points", %{config: config} do
      # Create a timeline with some data points and some empty ones
      base_time = ~U[2025-08-18 10:00:00Z]
      
      # Track data only on specific hours  
      Trifle.Stats.track("page_views", base_time, %{"count" => 5}, config)
      Trifle.Stats.track("page_views", DateTime.add(base_time, 2, :hour), %{"count" => 3}, config)
      
      # Query over a 4-hour granularity with skip_blanks
      result = Trifle.Stats.values("page_views", base_time, DateTime.add(base_time, 3, :hour), :hour, config, skip_blanks: true)
      
      # Should return only 2 time points (non-empty ones)
      assert length(result.at) == 2
      assert length(result.values) == 2
      
      # All returned values should be non-empty
      assert Enum.all?(result.values, fn value -> map_size(value) > 0 end)
      
      # Check the specific timestamps are correct (compare as Unix timestamps)
      expected_times = [base_time, DateTime.add(base_time, 2, :hour)]
      assert Enum.map(result.at, &DateTime.to_unix/1) == Enum.map(expected_times, &DateTime.to_unix/1)
    end
    
    test "values with skip_blanks handles all empty data correctly", %{config: config} do
      base_time = ~U[2025-08-18 10:00:00Z]
      
      # Don't track any data, just query
      result = Trifle.Stats.values("never_tracked", base_time, DateTime.add(base_time, 2, :hour), :hour, config, skip_blanks: true)
      
      # Should return empty arrays
      assert result.at == []
      assert result.values == []
    end
    
    test "values with skip_blanks handles all non-empty data correctly", %{config: config} do
      base_time = ~U[2025-08-18 10:00:00Z]
      
      # Track data on all hours in the granularity
      Trifle.Stats.track("page_views", base_time, %{"count" => 1}, config)
      Trifle.Stats.track("page_views", DateTime.add(base_time, 1, :hour), %{"count" => 2}, config)
      Trifle.Stats.track("page_views", DateTime.add(base_time, 2, :hour), %{"count" => 3}, config)
      
      result = Trifle.Stats.values("page_views", base_time, DateTime.add(base_time, 2, :hour), :hour, config, skip_blanks: true)
      
      # Should return all 3 time points since none are empty
      assert length(result.at) == 3
      assert length(result.values) == 3
      
      # All values should be non-empty
      assert Enum.all?(result.values, fn value -> map_size(value) > 0 end)
    end
    
    test "skip_blanks works with different granularities", %{config: config} do
      base_time = ~U[2025-08-18 00:00:00Z]
      
      # Track data only on specific days
      Trifle.Stats.track("daily_views", base_time, %{"count" => 10}, config)
      Trifle.Stats.track("daily_views", DateTime.add(base_time, 3, :day), %{"count" => 15}, config)
      
      # Query over a 5-day granularity
      result = Trifle.Stats.values("daily_views", base_time, DateTime.add(base_time, 4, :day), :day, config, skip_blanks: true)
      
      # Should return only 2 days (the ones with data)
      assert length(result.at) == 2
      assert length(result.values) == 2
      
      # Verify the correct days are returned (compare as Unix timestamps)
      expected_days = [base_time, DateTime.add(base_time, 3, :day)]
      assert Enum.map(result.at, &DateTime.to_unix/1) == Enum.map(expected_days, &DateTime.to_unix/1)
    end
    
    test "skip_blanks preserves zero values correctly", %{config: config} do
      base_time = ~U[2025-08-18 10:00:00Z]
      
      # Track data including zero values (should NOT be considered blank)
      Trifle.Stats.track("page_views", base_time, %{"count" => 0, "errors" => 0}, config)
      Trifle.Stats.track("page_views", DateTime.add(base_time, 2, :hour), %{"count" => 5}, config)
      
      result = Trifle.Stats.values("page_views", base_time, DateTime.add(base_time, 3, :hour), :hour, config, skip_blanks: true)
      
      # Should return 2 time points (zero values are not considered blank)
      assert length(result.at) == 2
      assert length(result.values) == 2
      
      # First value should have zero values but still be included
      first_value = Enum.at(result.values, 0)
      assert first_value["count"] == 0
      assert first_value["errors"] == 0
    end
    
    test "backward compatibility - values without opts still works", %{config: config} do
      base_time = ~U[2025-08-18 10:00:00Z]
      
      Trifle.Stats.track("page_views", base_time, %{"count" => 5}, config)
      
      # Call without opts parameter (should default to skip_blanks: false)
      result = Trifle.Stats.values("page_views", base_time, base_time, :hour, config)
      
      assert is_map(result)
      assert Map.has_key?(result, :at)
      assert Map.has_key?(result, :values)
    end
    
    test "values with explicit skip_blanks: false behaves like default", %{config: config} do
      base_time = ~U[2025-08-18 10:00:00Z]
      
      Trifle.Stats.track("page_views", base_time, %{"count" => 5}, config)
      
      result_default = Trifle.Stats.values("page_views", base_time, DateTime.add(base_time, 2, :hour), :hour, config)
      result_explicit = Trifle.Stats.values("page_views", base_time, DateTime.add(base_time, 2, :hour), :hour, config, skip_blanks: false)
      
      # Both should be identical
      assert result_default == result_explicit
    end
  end
end