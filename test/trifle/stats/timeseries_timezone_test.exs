defmodule Trifle.Stats.TimeseriesTimezoneTest do
  use ExUnit.Case

  test "track floors daily buckets in configured timezone" do
    {:ok, pid} = Trifle.Stats.Driver.Process.start_link()
    driver = Trifle.Stats.Driver.Process.new(pid)

    config =
      Trifle.Stats.Configuration.configure(
        driver,
        time_zone: "Etc/UTC",
        time_zone_database: Tzdata.TimeZoneDatabase,
        track_granularities: ["1d"],
        buffer_enabled: false
      )

    {:ok, at_utc, _} = DateTime.from_iso8601("2026-02-24T20:30:00Z")
    at_plus4 = DateTime.shift_zone!(at_utc, "Etc/GMT-4", Tzdata.TimeZoneDatabase)

    :ok = Trifle.Stats.track("tz_metric", at_plus4, %{"count" => 1}, config)

    from = ~U[2026-02-24 00:00:00Z]
    to = ~U[2026-02-25 00:00:00Z]

    result = Trifle.Stats.values("tz_metric", from, to, "1d", config)

    assert Enum.map(result.at, &DateTime.to_unix/1) == Enum.map([from, to], &DateTime.to_unix/1)
    assert Enum.at(result.values, 0)["count"] == 1
    assert Enum.at(result.values, 1) == %{}

    GenServer.stop(pid)
  end
end
