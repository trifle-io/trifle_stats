defmodule Trifle.Stats.EnhancedConfigurationTest do
  use ExUnit.Case
  
  alias Trifle.Stats.Configuration
  alias Trifle.Stats.Driver

  describe "Ruby compatibility" do
    test "default configuration matches Ruby defaults" do
      {:ok, conn} = Driver.Process.start_link()
      driver = Driver.Process.new(conn)
      config = Configuration.configure(driver)
      
      # Ruby defaults
      assert config.time_zone == "GMT"
      assert config.beginning_of_week == :monday
      assert config.track_granularities == nil  # nil means use all granularities
      assert config.granularities == ["1m", "1h", "1d", "1w", "1mo", "1q", "1y"]
      assert config.designator == nil
    end
    
    test "track_granularities filtering works like Ruby" do
      {:ok, conn} = Driver.Process.start_link()
      driver = Driver.Process.new(conn)
      
      # Nil track_granularities -> all granularities (Ruby behavior)
      config = Configuration.configure(driver, track_granularities: nil)
      assert config.granularities == ["1m", "1h", "1d", "1w", "1mo", "1q", "1y"]
      
      # Empty track_granularities -> all granularities (Ruby behavior)  
      config = Configuration.configure(driver, track_granularities: [])
      assert config.granularities == ["1m", "1h", "1d", "1w", "1mo", "1q", "1y"]
      
      # Specific granularities -> intersection maintaining order (Ruby behavior)
      config = Configuration.configure(driver, track_granularities: ["1h", "1d", "1mo", "invalid_granularity"])
      assert config.granularities == ["1h", "1d", "1mo"]
      
      # Invalid granularities filtered out
      config = Configuration.configure(driver, track_granularities: ["invalid1", "invalid2"])
      assert config.granularities == []
    end
    
    test "timezone validation with warning on invalid timezone" do
      import ExUnit.CaptureIO
      
      {:ok, conn} = Driver.Process.start_link()
      driver = Driver.Process.new(conn)
      config = Configuration.configure(driver, time_zone: "Invalid/Timezone")
      
      output = capture_io(fn ->
        tz = Configuration.tz(config)
        assert tz.time_zone == "GMT"
      end)
      
      assert output =~ "Trifle: Invalid timezone Invalid/Timezone; Defaulting to GMT"
    end
    
    test "blank? function matches Ruby behavior" do
      # Ruby: truthy blank values
      assert Configuration.blank?(nil) == true
      assert Configuration.blank?(false) == true  
      assert Configuration.blank?([]) == true
      assert Configuration.blank?("") == true
      assert Configuration.blank?("   ") == true  # whitespace-only strings
      assert Configuration.blank?(%{}) == true
      
      # Ruby: falsy blank values
      assert Configuration.blank?([1, 2]) == false
      assert Configuration.blank?("hello") == false
      assert Configuration.blank?(true) == false
      assert Configuration.blank?(0) == false
      assert Configuration.blank?(42) == false
    end
  end
  
  describe "driver validation" do
    test "validates driver exists" do
      assert_raise Trifle.Stats.DriverNotFoundError, fn ->
        Configuration.configure(nil)
      end
    end
    
    test "validates driver has required functions" do
      # Invalid driver should raise error
      invalid_driver = %{__struct__: String}
      
      assert_raise Trifle.Stats.DriverNotFoundError, fn ->
        Configuration.configure(invalid_driver)
      end
    end
    
    test "can disable driver validation" do
      # Should not raise error when validation disabled
      config = Configuration.configure(nil, validate_driver: false)
      assert config.driver == nil
    end
    
    test "valid driver passes validation" do
      {:ok, conn} = Driver.Process.start_link()
      driver = Driver.Process.new(conn)
      config = Configuration.configure(driver)
      assert config.driver == driver
    end
  end

  describe "buffered storage" do
    test "uses buffer when enabled" do
      {:ok, conn} = Driver.Process.start_link()
      driver = Driver.Process.new(conn)

      config =
        Configuration.configure(driver,
          buffer_enabled: true,
          buffer_duration: 0.01,
          buffer_size: 10,
          buffer_aggregate: true
        )

      assert %Trifle.Stats.Buffer{} = config.storage
      assert %Trifle.Stats.Buffer{} = Configuration.storage(config)

      Trifle.Stats.Buffer.shutdown(config.storage)
    end

    test "falls back to driver when buffer disabled" do
      {:ok, conn} = Driver.Process.start_link()
      driver = Driver.Process.new(conn)
      config = Configuration.configure(driver, buffer_enabled: false)

      assert config.storage == driver
      assert Configuration.storage(config) == driver
    end
  end
  
  describe "driver-specific options" do
    test "MongoDB driver options" do
      {:ok, conn} = Agent.start_link(fn -> %{} end)
      driver = Driver.Mongo.new(conn)
      
      config = Configuration.configure(driver, driver_options: %{
        collection_name: "custom_analytics",
        joined_identifier: false,
        expire_after: 86400
      })
      
      assert Configuration.driver_option(config, :collection_name) == "custom_analytics"
      assert Configuration.driver_option(config, :joined_identifier) == false
      assert Configuration.driver_option(config, :expire_after) == 86400
    end
    
    test "Redis driver options" do
      {:ok, conn} = Agent.start_link(fn -> %{} end)
      driver = Driver.Redis.new(conn)
      
      config = Configuration.configure(driver, driver_options: %{
        prefix: "analytics"
      })
      
      assert Configuration.driver_option(config, :prefix) == "analytics"
      assert Configuration.driver_option(config, :nonexistent, "default") == "default"
    end
    
    test "PostgreSQL driver options" do
      {:ok, conn} = Agent.start_link(fn -> %{} end) 
      driver = Driver.Postgres.new(conn)
      
      config = Configuration.configure(driver, driver_options: %{
        table_name: "analytics_stats",
        ping_table_name: "analytics_ping",
        joined_identifier: false
      })
      
      assert Configuration.driver_option(config, :table_name) == "analytics_stats"
      assert Configuration.driver_option(config, :ping_table_name) == "analytics_ping"
      assert Configuration.driver_option(config, :joined_identifier) == false
    end
    
    test "merge driver options" do
      {:ok, conn} = Driver.Process.start_link()
      driver = Driver.Process.new(conn)
      config = Configuration.configure(driver, driver_options: %{a: 1, b: 2})
      
      updated = Configuration.merge_driver_options(config, %{b: 3, c: 4})
      
      assert updated.driver_options == %{a: 1, b: 3, c: 4}
    end
  end
  
  describe "timezone handling" do
    test "valid timezone returns timezone object" do
      {:ok, conn} = Driver.Process.start_link()
      driver = Driver.Process.new(conn)
      config = Configuration.configure(driver, time_zone: "Europe/London")
      
      tz = Configuration.tz(config)
      assert tz.time_zone == "Europe/London"
      assert tz.database != nil
    end
    
    test "GMT timezone works" do
      {:ok, conn} = Driver.Process.start_link()
      driver = Driver.Process.new(conn)
      config = Configuration.configure(driver, time_zone: "GMT")
      
      tz = Configuration.tz(config)
      assert tz.time_zone == "GMT"
    end
    
    test "UTC timezone works" do
      {:ok, conn} = Driver.Process.start_link()
      driver = Driver.Process.new(conn)
      config = Configuration.configure(driver, time_zone: "UTC")
      
      tz = Configuration.tz(config)
      assert tz.time_zone == "UTC"
    end
  end
  
  describe "configuration API compatibility" do
    test "setter functions work" do
      {:ok, conn} = Driver.Process.start_link()
      driver = Driver.Process.new(conn)
      config = Configuration.configure(driver)
      
      updated = config
      |> Configuration.set_time_zone("Europe/Berlin")
      |> Configuration.set_beginning_of_week(:sunday)
      |> Configuration.set_separator("::")
      |> Configuration.set_granularities(["1h", "1d"])
      
      assert updated.time_zone == "Europe/Berlin"
      assert updated.beginning_of_week == :sunday
      assert updated.separator == "::"
      assert Enum.sort(updated.granularities) == Enum.sort(["1h", "1d"])
    end
    
    test "backwards compatible configure function" do
      {:ok, conn} = Driver.Process.start_link()
      driver = Driver.Process.new(conn)
      
      # Old API should still work
      config = Configuration.configure(
        driver,
        "Europe/London",    # time_zone
        nil,                # time_zone_database
        :sunday,            # beginning_of_week
        ["1h", "1d"],      # track_granularities
        "::"                # separator
      )
      
      assert config.time_zone == "Europe/London"
      assert config.beginning_of_week == :sunday
      assert config.separator == "::"
    end
  end
  
  describe "global configuration" do
    setup do
      # Clear any existing global config
      Configuration.clear_global()
      
      on_exit(fn ->
        Configuration.clear_global()
      end)
    end
    
    test "configure_global stores configuration" do
      {:ok, conn} = Driver.Process.start_link()
      driver = Driver.Process.new(conn)
      
      config = Configuration.configure_global(
        driver: driver,
        time_zone: "Europe/Berlin",
        track_granularities: ["1h", "1d"]
      )
      
      assert config == Configuration.get_global()
    end
    
    test "resolve_config prefers passed config over global" do
      {:ok, conn1} = Driver.Process.start_link()
      {:ok, conn2} = Driver.Process.start_link()
      driver1 = Driver.Process.new(conn1)
      driver2 = Driver.Process.new(conn2)
      
      # Set global config
      Configuration.configure_global(driver: driver1)
      
      # Passed config should take precedence
      passed_config = Configuration.configure(driver2)
      resolved = Configuration.resolve_config(passed_config)
      
      assert resolved.driver == driver2
    end
    
    test "resolve_config uses global when no config passed" do
      {:ok, conn} = Driver.Process.start_link()
      driver = Driver.Process.new(conn)
      Configuration.configure_global(driver: driver, time_zone: "UTC")
      
      resolved = Configuration.resolve_config(nil)
      assert resolved.driver == driver
      assert resolved.time_zone == "UTC"
    end
    
    test "resolve_config raises helpful error when no config available" do
      assert_raise RuntimeError, ~r/No configuration provided/, fn ->
        Configuration.resolve_config(nil)
      end
    end
  end
  
  describe "Ruby gem compatibility scenarios" do
    test "exact Ruby configuration translation" do
      # Simulate Ruby configuration:
      # Trifle::Stats.configure do |c|
      #   c.driver = Trifle::Stats::Driver::Mongo.new(client, 
      #     collection_name: "analytics_stats",
      #     joined_identifier: false, 
      #     expire_after: 86400)
      #   c.time_zone = "Europe/London"
      #   c.track_granularities = ["1h", "1d", "1w"]
      #   c.beginning_of_week = :sunday
      # end
      
      {:ok, conn} = Agent.start_link(fn -> %{} end)
      
      config = Configuration.configure(
        Driver.Mongo.new(conn),
        time_zone: "Europe/London",
        track_granularities: ["1h", "1d", "1w"],
        beginning_of_week: :sunday,
        driver_options: %{
          collection_name: "analytics_stats",
          joined_identifier: false,
          expire_after: 86400
        }
      )
      
      # Verify all Ruby options translated correctly
      assert config.time_zone == "Europe/London"
      assert config.beginning_of_week == :sunday
      assert config.granularities == ["1h", "1d", "1w"]
      assert Configuration.driver_option(config, :collection_name) == "analytics_stats"
      assert Configuration.driver_option(config, :joined_identifier) == false
      assert Configuration.driver_option(config, :expire_after) == 86400
    end
    
    test "driver creation from config" do
      {:ok, conn} = Agent.start_link(fn -> %{} end)
      base_driver = Driver.Mongo.new(conn)
      
      config = Configuration.configure(base_driver, driver_options: %{
        collection_name: "custom_collection",
        expire_after: 3600
      })
      
      # Create driver from config (Ruby pattern)
      configured_driver = Driver.Mongo.from_config(conn, config)
      
      assert configured_driver.collection_name == "custom_collection"
      assert configured_driver.expire_after == 3600
    end
  end
end
