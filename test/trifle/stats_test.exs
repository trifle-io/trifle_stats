defmodule Trifle.StatsTest do
  use ExUnit.Case

  test "modules load and compile" do
    # Test that all main modules compile
    assert {:module, Trifle.Stats} = Code.ensure_loaded(Trifle.Stats)
    assert {:module, Trifle.Stats.Configuration} = Code.ensure_loaded(Trifle.Stats.Configuration)
    assert {:module, Trifle.Stats.Nocturnal} = Code.ensure_loaded(Trifle.Stats.Nocturnal)
    assert {:module, Trifle.Stats.Series} = Code.ensure_loaded(Trifle.Stats.Series)
  end

  test "timezone functionality works" do
    # Test timezone functions with proper config
    config = Trifle.Stats.Configuration.configure(
      nil,
      time_zone: "UTC",
      time_zone_database: Tzdata.TimeZoneDatabase,
      beginning_of_week: :monday,
      validate_driver: false
    )
    
    at = DateTime.utc_now()
    
    # Test new granularity API
    minute_parser = Trifle.Stats.Nocturnal.Parser.new("1m")
    hour_parser = Trifle.Stats.Nocturnal.Parser.new("1h")
    
    nocturnal = Trifle.Stats.Nocturnal.new(at, config)
    minute_boundary = Trifle.Stats.Nocturnal.floor(nocturnal, minute_parser.offset, minute_parser.unit)
    hour_boundary = Trifle.Stats.Nocturnal.floor(nocturnal, hour_parser.offset, hour_parser.unit)
    
    assert %DateTime{} = minute_boundary
    assert %DateTime{} = hour_boundary
    assert hour_boundary.minute == 0
  end
end