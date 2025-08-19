defmodule Trifle.Stats.GlobalConfigTest do
  use ExUnit.Case, async: false

  # Clear global config before each test to ensure isolation
  setup do
    Trifle.Stats.Configuration.clear_global()

    on_exit(fn ->
      Trifle.Stats.Configuration.clear_global()
    end)
  end

  describe "global configuration management" do
    test "configure_global/1 stores configuration in Application env" do
      {:ok, pid} = Trifle.Stats.Driver.Process.start_link()
      driver = Trifle.Stats.Driver.Process.new(pid)

      config =
        Trifle.Stats.Configuration.configure_global(
          driver: driver,
          time_zone: "UTC",
          time_zone_database: Tzdata.TimeZoneDatabase,
          track_granularities: [:hour, :day],
          beginning_of_week: :sunday
        )

      assert config.driver == driver
      assert config.time_zone == "UTC"
      assert config.beginning_of_week == :sunday
      assert Enum.sort(config.granularities) == Enum.sort([:hour, :day])
    end

    test "get_global/0 returns stored configuration" do
      assert Trifle.Stats.Configuration.get_global() == nil

      {:ok, pid} = Trifle.Stats.Driver.Process.start_link()
      driver = Trifle.Stats.Driver.Process.new(pid)
      expected_config = Trifle.Stats.Configuration.configure_global(driver: driver)

      retrieved_config = Trifle.Stats.Configuration.get_global()
      assert retrieved_config == expected_config
    end

    test "clear_global/0 removes configuration" do
      {:ok, pid} = Trifle.Stats.Driver.Process.start_link()
      driver = Trifle.Stats.Driver.Process.new(pid)
      Trifle.Stats.Configuration.configure_global(driver: driver)

      assert Trifle.Stats.Configuration.get_global() != nil

      Trifle.Stats.Configuration.clear_global()
      assert Trifle.Stats.Configuration.get_global() == nil
    end

    test "resolve_config/1 prefers passed config over global config" do
      {:ok, global_pid} = Trifle.Stats.Driver.Process.start_link()
      global_driver = Trifle.Stats.Driver.Process.new(global_pid)
      Trifle.Stats.Configuration.configure_global(driver: global_driver, time_zone: "UTC")

      {:ok, specific_pid} = Trifle.Stats.Driver.Process.start_link()
      specific_driver = Trifle.Stats.Driver.Process.new(specific_pid)

      specific_config =
        Trifle.Stats.Configuration.configure(specific_driver,
          time_zone: "UTC",
          time_zone_database: Tzdata.TimeZoneDatabase
        )

      # Should use specific config, not global
      result = Trifle.Stats.Configuration.resolve_config(specific_config)
      assert result.time_zone == "UTC"
      assert result.driver == specific_driver
    end

    test "resolve_config/1 uses global config when no config passed" do
      {:ok, pid} = Trifle.Stats.Driver.Process.start_link()
      driver = Trifle.Stats.Driver.Process.new(pid)
      Trifle.Stats.Configuration.configure_global(driver: driver, time_zone: "UTC")

      result = Trifle.Stats.Configuration.resolve_config(nil)
      assert result.time_zone == "UTC"
      assert result.driver == driver
    end

    test "resolve_config/1 raises error when no config available" do
      assert_raise RuntimeError, ~r/No configuration provided/, fn ->
        Trifle.Stats.Configuration.resolve_config(nil)
      end
    end

    test "Trifle.Stats.configure/1 is shorthand for configure_global/1" do
      {:ok, pid} = Trifle.Stats.Driver.Process.start_link()
      driver = Trifle.Stats.Driver.Process.new(pid)

      config =
        Trifle.Stats.configure(
          driver: driver,
          time_zone: "UTC",
          time_zone_database: Tzdata.TimeZoneDatabase
        )

      assert config.time_zone == "UTC"
      assert Trifle.Stats.Configuration.get_global() == config
    end

    test "can use Trifle.Stats functions without passing config after global setup" do
      # This should work without raising errors after global config is set
      {:ok, pid} = Trifle.Stats.Driver.Process.start_link()
      driver = Trifle.Stats.Driver.Process.new(pid)

      Trifle.Stats.configure(
        driver: driver,
        time_zone: "UTC",
        time_zone_database: Tzdata.TimeZoneDatabase
      )

      # These should not raise "No configuration provided" errors
      assert :ok = Trifle.Stats.track("test_key", DateTime.utc_now(), %{count: 1})
      assert :ok = Trifle.Stats.assert("test_key", DateTime.utc_now(), %{count: 5})

      assert %{at: _, values: _} =
               Trifle.Stats.values("test_key", DateTime.utc_now(), DateTime.utc_now(), :day)

      assert :ok = Trifle.Stats.assort("test_key", DateTime.utc_now(), %{duration: 100})
      assert :ok = Trifle.Stats.beam("test_key", DateTime.utc_now(), %{status: "ok"})
      assert %{at: _, values: _} = Trifle.Stats.scan("test_key")
    end

    test "explicit config still works when global config is set" do
      # Set global config  
      {:ok, global_pid} = Trifle.Stats.Driver.Process.start_link()
      global_driver = Trifle.Stats.Driver.Process.new(global_pid)

      Trifle.Stats.configure(
        driver: global_driver,
        time_zone: "UTC",
        time_zone_database: Tzdata.TimeZoneDatabase,
        time_zone_database: Calendar.get_time_zone_database()
      )

      # Use explicit config with different settings
      {:ok, specific_pid} = Trifle.Stats.Driver.Process.start_link()
      specific_driver = Trifle.Stats.Driver.Process.new(specific_pid)

      specific_config =
        Trifle.Stats.Configuration.configure(specific_driver,
          time_zone: "UTC",
          time_zone_database: Tzdata.TimeZoneDatabase,
          time_zone_database: Calendar.get_time_zone_database()
        )

      # Should work with explicit config
      assert :ok =
               Trifle.Stats.track("test_key", DateTime.utc_now(), %{count: 1}, specific_config)
    end
  end

  describe "backwards compatibility" do
    test "old configure/6 API still works" do
      {:ok, pid} = Trifle.Stats.Driver.Process.start_link()
      driver = Trifle.Stats.Driver.Process.new(pid)

      config =
        Trifle.Stats.Configuration.configure(
          driver,
          # time_zone
          "Etc/UTC",
          # time_zone_database
          nil,
          # beginning_of_week
          :sunday,
          # track_granularities
          [:hour, :day],
          # separator
          "|"
        )

      assert config.driver == driver
      assert config.time_zone == "Etc/UTC"
      assert config.beginning_of_week == :sunday
      assert config.separator == "|"
      assert Enum.sort(config.granularities) == Enum.sort([:hour, :day])
    end

    test "new configure/2 keyword API works" do
      {:ok, pid} = Trifle.Stats.Driver.Process.start_link()
      driver = Trifle.Stats.Driver.Process.new(pid)

      config =
        Trifle.Stats.Configuration.configure(driver,
          time_zone: "UTC",
          time_zone_database: Tzdata.TimeZoneDatabase,
          beginning_of_week: :sunday,
          track_granularities: [:hour, :day],
          separator: "|"
        )

      assert config.driver == driver
      assert config.time_zone == "UTC"
      assert config.beginning_of_week == :sunday
      assert config.separator == "|"
      assert Enum.sort(config.granularities) == Enum.sort([:hour, :day])
    end
  end
end
