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
    {:ok, minute_boundary} = Trifle.Stats.Nocturnal.minute(at, config)
    {:ok, hour_boundary} = Trifle.Stats.Nocturnal.hour(at, config)
    
    assert %DateTime{} = minute_boundary
    assert %DateTime{} = hour_boundary
    assert hour_boundary.minute == 0
  end
end