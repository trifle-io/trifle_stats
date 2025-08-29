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
      nocturnal = Trifle.Stats.Nocturnal.new(at, config)
      
      # Test minute boundary
      minute_parser = Trifle.Stats.Nocturnal.Parser.new("1m")
      minute_boundary = Trifle.Stats.Nocturnal.floor(nocturnal, minute_parser.offset, minute_parser.unit)
      assert minute_boundary.second == 0
      assert minute_boundary.minute == at.minute
      
      # Test hour boundary
      hour_parser = Trifle.Stats.Nocturnal.Parser.new("1h")
      hour_boundary = Trifle.Stats.Nocturnal.floor(nocturnal, hour_parser.offset, hour_parser.unit)
      assert hour_boundary.minute == 0
      assert hour_boundary.second == 0
      assert hour_boundary.hour == at.hour
      
      # Test day boundary
      day_parser = Trifle.Stats.Nocturnal.Parser.new("1d")
      day_boundary = Trifle.Stats.Nocturnal.floor(nocturnal, day_parser.offset, day_parser.unit)
      assert day_boundary.hour == 0
      assert day_boundary.minute == 0
      assert day_boundary.second == 0
      assert day_boundary.day == at.day
    end
    
    test "next time functions work", %{config: config} do
      at = ~U[2025-08-17 14:35:42Z]
      nocturnal = Trifle.Stats.Nocturnal.new(at, config)
      
      # Test next minute
      minute_parser = Trifle.Stats.Nocturnal.Parser.new("1m")
      next_minute = Trifle.Stats.Nocturnal.add(nocturnal, minute_parser.offset, minute_parser.unit)
      assert next_minute.minute == at.minute + 1 || (next_minute.minute == 0 && next_minute.hour == at.hour + 1)
      
      # Test next hour
      hour_parser = Trifle.Stats.Nocturnal.Parser.new("1h")
      next_hour = Trifle.Stats.Nocturnal.add(nocturnal, hour_parser.offset, hour_parser.unit)
      assert next_hour.hour == at.hour + 1 || (next_hour.hour == 0 && next_hour.day == at.day + 1)
      
      # Test next day
      day_parser = Trifle.Stats.Nocturnal.Parser.new("1d")
      next_day = Trifle.Stats.Nocturnal.add(nocturnal, day_parser.offset, day_parser.unit)
      assert next_day.day == at.day + 1 || next_day.month == at.month + 1
    end
    
    test "timeline generation works", %{config: config} do
      from = ~U[2025-08-17 10:00:00Z]
      to = ~U[2025-08-17 13:00:00Z]
      
      timeline = Trifle.Stats.Nocturnal.timeline(from: from, to: to, offset: 1, unit: :hour, config: config)
      
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
      
      nocturnal = Trifle.Stats.Nocturnal.new(sunday, config)
      week_parser = Trifle.Stats.Nocturnal.Parser.new("1w")
      week_boundary = Trifle.Stats.Nocturnal.floor(nocturnal, week_parser.offset, week_parser.unit)
      
      # Should be start of Monday (since config has beginning_of_week: :monday)
      assert Date.day_of_week(week_boundary) == 1  # Monday
      assert week_boundary.hour == 0
      assert week_boundary.minute == 0
      assert week_boundary.second == 0
    end
  end
end