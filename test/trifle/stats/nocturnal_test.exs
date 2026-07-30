defmodule Trifle.Stats.NocturnalTest do
  use ExUnit.Case

  describe "timezone and time boundary functions" do
    setup do
      config =
        Trifle.Stats.Configuration.configure(
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

      minute_boundary =
        Trifle.Stats.Nocturnal.floor(nocturnal, minute_parser.offset, minute_parser.unit)

      assert minute_boundary.second == 0
      assert minute_boundary.minute == at.minute

      # Test hour boundary
      hour_parser = Trifle.Stats.Nocturnal.Parser.new("1h")

      hour_boundary =
        Trifle.Stats.Nocturnal.floor(nocturnal, hour_parser.offset, hour_parser.unit)

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

      next_minute =
        Trifle.Stats.Nocturnal.add(nocturnal, minute_parser.offset, minute_parser.unit)

      assert next_minute.minute == at.minute + 1 ||
               (next_minute.minute == 0 && next_minute.hour == at.hour + 1)

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

      timeline =
        Trifle.Stats.Nocturnal.timeline(
          from: from,
          to: to,
          offset: 1,
          unit: :hour,
          config: config
        )

      assert is_list(timeline)
      # 10, 11, 12, 13
      assert length(timeline) == 4

      # Verify all entries are hour boundaries
      Enum.each(timeline, fn dt ->
        assert dt.minute == 0
        assert dt.second == 0
      end)
    end

    test "dynamic segments re-anchor when they cross their enclosing boundary", %{
      config: config
    } do
      timeline =
        Trifle.Stats.Nocturnal.timeline(
          from: ~U[2025-08-17 10:37:00Z],
          to: ~U[2025-08-17 11:10:00Z],
          offset: 33,
          unit: :minute,
          config: config
        )

      assert Enum.map(timeline, &DateTime.to_unix/1) == [1_755_426_780, 1_755_428_400]
      assert Enum.all?(timeline, &(&1.time_zone == "UTC"))
    end

    test "week calculations work", %{config: config} do
      # Test with different days of week
      # This is a Sunday
      sunday = ~U[2025-08-17 14:35:42Z]

      nocturnal = Trifle.Stats.Nocturnal.new(sunday, config)
      week_parser = Trifle.Stats.Nocturnal.Parser.new("1w")

      week_boundary =
        Trifle.Stats.Nocturnal.floor(nocturnal, week_parser.offset, week_parser.unit)

      # Should be start of Monday (since config has beginning_of_week: :monday)
      # Monday
      assert Date.day_of_week(week_boundary) == 1
      assert week_boundary.hour == 0
      assert week_boundary.minute == 0
      assert week_boundary.second == 0
    end
  end

  describe "daylight saving transitions" do
    setup do
      config =
        Trifle.Stats.Configuration.configure(
          nil,
          time_zone: "Europe/Bratislava",
          time_zone_database: Tzdata.TimeZoneDatabase,
          beginning_of_week: :monday,
          validate_driver: false
        )

      {:ok, config: config}
    end

    test "keeps daily timelines on local midnight across spring daylight saving time", %{
      config: config
    } do
      timeline =
        Trifle.Stats.Nocturnal.timeline(
          from: zoned!(~D[2026-01-01], ~T[00:00:00]),
          to: zoned!(~D[2026-07-20], ~T[23:59:59]),
          offset: 1,
          unit: :day,
          config: config
        )

      tracked_bucket =
        zoned!(~D[2026-07-20], ~T[12:00:00])
        |> Trifle.Stats.Nocturnal.new(config)
        |> Trifle.Stats.Nocturnal.floor(1, :day)

      assert length(timeline) == 201
      assert Enum.at(timeline, 88) == zoned!(~D[2026-03-30], ~T[00:00:00])
      assert List.last(timeline) == zoned!(~D[2026-07-20], ~T[00:00:00])
      assert tracked_bucket in timeline
      assert Enum.all?(timeline, &(&1.hour == 0))
    end

    test "represents both occurrences of a repeated fallback hour", %{config: config} do
      timeline =
        Trifle.Stats.Nocturnal.timeline(
          from: zoned!(~D[2026-10-25], ~T[00:00:00]),
          to: zoned!(~D[2026-10-25], ~T[04:00:00]),
          offset: 1,
          unit: :hour,
          config: config
        )

      assert Enum.map(timeline, &{&1.hour, total_offset(&1)}) == [
               {0, 7200},
               {1, 7200},
               {2, 7200},
               {2, 3600},
               {3, 3600},
               {4, 3600}
             ]
    end

    test "re-anchors multi-hour buckets at midnight on a 25-hour day", %{config: config} do
      timeline =
        Trifle.Stats.Nocturnal.timeline(
          from: zoned!(~D[2026-10-25], ~T[00:00:00]),
          to: zoned!(~D[2026-10-26], ~T[00:00:00]),
          offset: 6,
          unit: :hour,
          config: config
        )

      assert Enum.map(timeline, &{&1.day, &1.hour, total_offset(&1)}) == [
               {25, 0, 7200},
               {25, 5, 3600},
               {25, 11, 3600},
               {25, 17, 3600},
               {25, 23, 3600},
               {26, 0, 3600}
             ]

      assert Enum.all?(timeline, fn at ->
               at
               |> Trifle.Stats.Nocturnal.new(config)
               |> Trifle.Stats.Nocturnal.floor(6, :hour)
               |> DateTime.compare(at) == :eq
             end)
    end

    test "skips the nonexistent spring hour for elapsed hourly buckets", %{config: config} do
      timeline =
        Trifle.Stats.Nocturnal.timeline(
          from: zoned!(~D[2026-03-29], ~T[00:00:00]),
          to: zoned!(~D[2026-03-29], ~T[05:00:00]),
          offset: 1,
          unit: :hour,
          config: config
        )

      assert Enum.map(timeline, &{&1.hour, total_offset(&1)}) == [
               {0, 3600},
               {1, 3600},
               {3, 7200},
               {4, 7200},
               {5, 7200}
             ]
    end

    test "floors exact weekly and yearly boundaries using their target dates", %{config: config} do
      week = zoned!(~D[2026-04-06], ~T[00:00:00])
      two_week_boundary = zoned!(~D[2026-03-30], ~T[00:00:00])
      summer = zoned!(~D[2026-07-20], ~T[12:00:00])

      assert week
             |> Trifle.Stats.Nocturnal.new(config)
             |> Trifle.Stats.Nocturnal.floor(1, :week) == week

      assert two_week_boundary
             |> Trifle.Stats.Nocturnal.new(config)
             |> Trifle.Stats.Nocturnal.floor(2, :week) == two_week_boundary

      assert summer
             |> Trifle.Stats.Nocturnal.new(config)
             |> Trifle.Stats.Nocturnal.floor(1, :year) ==
               zoned!(~D[2026-01-01], ~T[00:00:00])

      assert summer
             |> Trifle.Stats.Nocturnal.new(config)
             |> Trifle.Stats.Nocturnal.floor(1, :quarter) ==
               zoned!(~D[2026-07-01], ~T[00:00:00])
    end

    test "uses calendar offsets for months in both directions", %{config: config} do
      january = zoned!(~D[2026-01-31], ~T[12:00:00])
      july = zoned!(~D[2026-07-31], ~T[12:00:00])

      assert january
             |> Trifle.Stats.Nocturnal.new(config)
             |> Trifle.Stats.Nocturnal.add(6, :month) == july

      assert january
             |> Trifle.Stats.Nocturnal.new(config)
             |> Trifle.Stats.Nocturnal.add(-2, :month) ==
               zoned!(~D[2025-11-30], ~T[12:00:00])
    end

    test "shifts nonexistent calendar times forward by the transition gap", %{config: config} do
      result =
        zoned!(~D[2026-03-28], ~T[02:30:00])
        |> Trifle.Stats.Nocturnal.new(config)
        |> Trifle.Stats.Nocturnal.add(1, :day)

      assert result == zoned!(~D[2026-03-29], ~T[03:30:00])
    end

    test "uses the actual transition size instead of assuming a one-hour gap" do
      config =
        Trifle.Stats.Configuration.configure(
          nil,
          time_zone: "Australia/Lord_Howe",
          time_zone_database: Tzdata.TimeZoneDatabase,
          beginning_of_week: :monday,
          validate_driver: false
        )

      result =
        DateTime.new!(
          ~D[2026-10-03],
          ~T[02:15:00],
          "Australia/Lord_Howe",
          Tzdata.TimeZoneDatabase
        )
        |> Trifle.Stats.Nocturnal.new(config)
        |> Trifle.Stats.Nocturnal.add(1, :day)

      assert result ==
               DateTime.new!(
                 ~D[2026-10-04],
                 ~T[02:45:00],
                 "Australia/Lord_Howe",
                 Tzdata.TimeZoneDatabase
               )
    end

    test "uses the source offset to resolve ambiguous calendar times", %{config: config} do
      summer =
        zoned!(~D[2026-10-24], ~T[02:30:00])
        |> Trifle.Stats.Nocturnal.new(config)
        |> Trifle.Stats.Nocturnal.add(1, :day)

      winter =
        zoned!(~D[2026-10-26], ~T[02:30:00])
        |> Trifle.Stats.Nocturnal.new(config)
        |> Trifle.Stats.Nocturnal.add(-1, :day)

      {:ambiguous, first, second} =
        DateTime.new(
          ~D[2026-10-25],
          ~T[02:30:00],
          "Europe/Bratislava",
          Tzdata.TimeZoneDatabase
        )

      assert summer == first
      assert winter == second
    end
  end

  defp zoned!(date, time) do
    DateTime.new!(date, time, "Europe/Bratislava", Tzdata.TimeZoneDatabase)
  end

  defp total_offset(datetime), do: datetime.utc_offset + datetime.std_offset
end
