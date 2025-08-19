defmodule Trifle.Stats.NocturnalTest do
  use ExUnit.Case

  describe "timezone and time boundary functions" do
    setup do
      config = Trifle.Stats.Configuration.configure(
        nil,
        time_zone: "UTC",
        time_zone_database: Tzdata.TimeZoneDatabase,
        beginning_of_week: :monday,
        validate_driver: false
      )
      
      {:ok, config: config}
    end
    
    test "basic time boundary functions work", %{config: config} do
      at = ~U[2025-08-17 14:35:42Z]
      
      # Test minute boundary
      {:ok, minute_boundary} = Trifle.Stats.Nocturnal.minute(at, config)
      assert minute_boundary.second == 0
      assert minute_boundary.minute == at.minute
      
      # Test hour boundary
      {:ok, hour_boundary} = Trifle.Stats.Nocturnal.hour(at, config)
      assert hour_boundary.minute == 0
      assert hour_boundary.second == 0
      assert hour_boundary.hour == at.hour
      
      # Test day boundary
      {:ok, day_boundary} = Trifle.Stats.Nocturnal.day(at, config)
      assert day_boundary.hour == 0
      assert day_boundary.minute == 0
      assert day_boundary.second == 0
      assert day_boundary.day == at.day
    end
    
    test "next time functions work", %{config: config} do
      at = ~U[2025-08-17 14:35:42Z]
      
      # Test next minute
      {:ok, next_minute} = Trifle.Stats.Nocturnal.next_minute(at, config)
      assert next_minute.minute == at.minute + 1 || (next_minute.minute == 0 && next_minute.hour == at.hour + 1)
      
      # Test next hour
      {:ok, next_hour} = Trifle.Stats.Nocturnal.next_hour(at, config)
      assert next_hour.hour == at.hour + 1 || (next_hour.hour == 0 && next_hour.day == at.day + 1)
      
      # Test next day
      {:ok, next_day} = Trifle.Stats.Nocturnal.next_day(at, config)
      assert next_day.day == at.day + 1 || next_day.month == at.month + 1
    end
    
    test "timeline generation works", %{config: config} do
      from = ~U[2025-08-17 10:00:00Z]
      to = ~U[2025-08-17 13:00:00Z]
      
      timeline = Trifle.Stats.Nocturnal.timeline(from, to, :hour, config)
      
      assert is_list(timeline)
      assert length(timeline) == 4  # 10, 11, 12, 13
      
      # Verify all entries are hour boundaries
      Enum.each(timeline, fn dt ->
        assert dt.minute == 0
        assert dt.second == 0
      end)
    end
    
    test "week calculations work", %{config: config} do
      # Test with different days of week
      sunday = ~U[2025-08-17 14:35:42Z]  # This is a Sunday
      
      {:ok, week_boundary} = Trifle.Stats.Nocturnal.week(sunday, config)
      
      # Should be start of Monday (since config has beginning_of_week: :monday)
      assert Date.day_of_week(week_boundary) == 1  # Monday
      assert week_boundary.hour == 0
      assert week_boundary.minute == 0
      assert week_boundary.second == 0
    end
  end
end