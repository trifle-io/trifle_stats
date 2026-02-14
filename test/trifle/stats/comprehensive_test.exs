defmodule Trifle.Stats.ComprehensiveTest do
  use ExUnit.Case

  # Helper function to convert array keys to Nocturnal::Key objects
  defp to_key([key_name, granularity, timestamp]) when is_binary(timestamp) do
    Trifle.Stats.Nocturnal.Key.new(
      key: key_name,
      granularity: granularity,
      at: DateTime.from_unix!(String.to_integer(timestamp))
    )
  end

  defp to_key([key_name, granularity, timestamp]) when is_integer(timestamp) do
    Trifle.Stats.Nocturnal.Key.new(
      key: key_name,
      granularity: granularity,
      at: DateTime.from_unix!(timestamp)
    )
  end

  defp to_keys(array_keys) do
    Enum.map(array_keys, &to_key/1)
  end

  describe "comprehensive functionality tests" do
    test "modules compile and load successfully" do
      # Core modules
      assert {:module, Trifle.Stats} = Code.ensure_loaded(Trifle.Stats)

      assert {:module, Trifle.Stats.Configuration} =
               Code.ensure_loaded(Trifle.Stats.Configuration)

      assert {:module, Trifle.Stats.Nocturnal} = Code.ensure_loaded(Trifle.Stats.Nocturnal)
      assert {:module, Trifle.Stats.Series} = Code.ensure_loaded(Trifle.Stats.Series)
      assert {:module, Trifle.Stats.Packer} = Code.ensure_loaded(Trifle.Stats.Packer)

      # Drivers  
      assert {:module, Trifle.Stats.Driver.Mongo} = Code.ensure_loaded(Trifle.Stats.Driver.Mongo)
      assert {:module, Trifle.Stats.Driver.Mysql} = Code.ensure_loaded(Trifle.Stats.Driver.Mysql)

      assert {:module, Trifle.Stats.Driver.Postgres} =
               Code.ensure_loaded(Trifle.Stats.Driver.Postgres)

      assert {:module, Trifle.Stats.Driver.Redis} = Code.ensure_loaded(Trifle.Stats.Driver.Redis)

      assert {:module, Trifle.Stats.Driver.Sqlite} =
               Code.ensure_loaded(Trifle.Stats.Driver.Sqlite)

      # Aggregators
      assert {:module, Trifle.Stats.Aggregator.Mean} =
               Code.ensure_loaded(Trifle.Stats.Aggregator.Mean)

      assert {:module, Trifle.Stats.Aggregator.Max} =
               Code.ensure_loaded(Trifle.Stats.Aggregator.Max)

      assert {:module, Trifle.Stats.Aggregator.Min} =
               Code.ensure_loaded(Trifle.Stats.Aggregator.Min)

      assert {:module, Trifle.Stats.Aggregator.Sum} =
               Code.ensure_loaded(Trifle.Stats.Aggregator.Sum)

      # Designators
      assert {:module, Trifle.Stats.Designator.Custom} =
               Code.ensure_loaded(Trifle.Stats.Designator.Custom)

      assert {:module, Trifle.Stats.Designator.Geometric} =
               Code.ensure_loaded(Trifle.Stats.Designator.Geometric)

      assert {:module, Trifle.Stats.Designator.Linear} =
               Code.ensure_loaded(Trifle.Stats.Designator.Linear)

      # Formatters
      assert {:module, Trifle.Stats.Formatter.Category} =
               Code.ensure_loaded(Trifle.Stats.Formatter.Category)

      assert {:module, Trifle.Stats.Formatter.Timeline} =
               Code.ensure_loaded(Trifle.Stats.Formatter.Timeline)

      # Transponders
      assert {:module, Trifle.Stats.Transponder.Add} =
               Code.ensure_loaded(Trifle.Stats.Transponder.Add)

      assert {:module, Trifle.Stats.Transponder.Divide} =
               Code.ensure_loaded(Trifle.Stats.Transponder.Divide)

      assert {:module, Trifle.Stats.Transponder.Max} =
               Code.ensure_loaded(Trifle.Stats.Transponder.Max)

      assert {:module, Trifle.Stats.Transponder.Mean} =
               Code.ensure_loaded(Trifle.Stats.Transponder.Mean)

      assert {:module, Trifle.Stats.Transponder.Min} =
               Code.ensure_loaded(Trifle.Stats.Transponder.Min)

      assert {:module, Trifle.Stats.Transponder.Multiply} =
               Code.ensure_loaded(Trifle.Stats.Transponder.Multiply)

      assert {:module, Trifle.Stats.Transponder.Ratio} =
               Code.ensure_loaded(Trifle.Stats.Transponder.Ratio)

      assert {:module, Trifle.Stats.Transponder.StandardDeviation} =
               Code.ensure_loaded(Trifle.Stats.Transponder.StandardDeviation)

      # Operations
      assert {:module, Trifle.Stats.Operations.Timeseries.Increment} =
               Code.ensure_loaded(Trifle.Stats.Operations.Timeseries.Increment)

      assert {:module, Trifle.Stats.Operations.Timeseries.Set} =
               Code.ensure_loaded(Trifle.Stats.Operations.Timeseries.Set)

      assert {:module, Trifle.Stats.Operations.Timeseries.Values} =
               Code.ensure_loaded(Trifle.Stats.Operations.Timeseries.Values)

      assert {:module, Trifle.Stats.Operations.Status.Beam} =
               Code.ensure_loaded(Trifle.Stats.Operations.Status.Beam)

      assert {:module, Trifle.Stats.Operations.Status.Scan} =
               Code.ensure_loaded(Trifle.Stats.Operations.Status.Scan)
    end

    test "timezone functionality works correctly" do
      config =
        Trifle.Stats.Configuration.configure(
          nil,
          time_zone: "UTC",
          time_zone_database: Tzdata.TimeZoneDatabase,
          beginning_of_week: :monday,
          validate_driver: false
        )

      at = DateTime.utc_now()

      # Test time boundary calculations with new API
      nocturnal = Trifle.Stats.Nocturnal.new(at, config)

      minute_parser = Trifle.Stats.Nocturnal.Parser.new("1m")

      minute_boundary =
        Trifle.Stats.Nocturnal.floor(nocturnal, minute_parser.offset, minute_parser.unit)

      hour_parser = Trifle.Stats.Nocturnal.Parser.new("1h")

      hour_boundary =
        Trifle.Stats.Nocturnal.floor(nocturnal, hour_parser.offset, hour_parser.unit)

      day_parser = Trifle.Stats.Nocturnal.Parser.new("1d")
      day_boundary = Trifle.Stats.Nocturnal.floor(nocturnal, day_parser.offset, day_parser.unit)

      assert %DateTime{} = minute_boundary
      assert %DateTime{} = hour_boundary
      assert %DateTime{} = day_boundary

      # Verify boundaries are properly zeroed
      assert minute_boundary.second == 0
      assert hour_boundary.minute == 0 and hour_boundary.second == 0
      assert day_boundary.hour == 0 and day_boundary.minute == 0 and day_boundary.second == 0

      # Test next calculations
      next_minute =
        Trifle.Stats.Nocturnal.add(nocturnal, minute_parser.offset, minute_parser.unit)

      next_hour = Trifle.Stats.Nocturnal.add(nocturnal, hour_parser.offset, hour_parser.unit)

      assert %DateTime{} = next_minute
      assert %DateTime{} = next_hour
      assert DateTime.compare(next_minute, minute_boundary) == :gt
      assert DateTime.compare(next_hour, hour_boundary) == :gt
    end

    test "designator functionality works" do
      # Test linear designator
      linear = Trifle.Stats.Designator.Linear.new(0, 100, 10)
      result = Trifle.Stats.Designator.Linear.designate(linear, 55)
      assert is_binary(result)

      # Test geometric designator  
      geometric = Trifle.Stats.Designator.Geometric.new(1, 2)
      result = Trifle.Stats.Designator.Geometric.designate(geometric, 16)
      assert is_binary(result)

      # Test custom designator
      custom = Trifle.Stats.Designator.Custom.new([0, 10, 25, 50, 100])
      result = Trifle.Stats.Designator.Custom.designate(custom, 30)
      assert is_binary(result)
    end

    test "aggregator functionality works with proper data structure" do
      # Aggregators expect data with :at and :values keys
      data = %{
        at: [~U[2025-08-17 10:00:00Z], ~U[2025-08-17 11:00:00Z], ~U[2025-08-17 12:00:00Z]],
        values: [%{count: 10}, %{count: 20}, %{count: 30}]
      }

      # Test mean aggregator
      mean_result = Trifle.Stats.Aggregator.Mean.aggregate(data, "count")
      assert is_number(mean_result) or is_list(mean_result)

      # Test sum aggregator
      sum_result = Trifle.Stats.Aggregator.Sum.aggregate(data, "count")
      assert is_number(sum_result) or is_list(sum_result)

      # Test max aggregator
      max_result = Trifle.Stats.Aggregator.Max.aggregate(data, "count")
      assert is_number(max_result) or is_list(max_result)

      # Test min aggregator
      min_result = Trifle.Stats.Aggregator.Min.aggregate(data, "count")
      assert is_number(min_result) or is_list(min_result)
    end

    test "formatter functionality works with proper data structure" do
      # Formatters expect data with :at and :values keys
      timeline_data = %{
        at: [~U[2025-08-17 10:00:00Z], ~U[2025-08-17 11:00:00Z]],
        values: [%{count: 5}, %{count: 10}]
      }

      # Test timeline formatter
      timeline_result = Trifle.Stats.Formatter.Timeline.format(timeline_data, "count")
      assert is_map(timeline_result)
      assert Map.has_key?(timeline_result, "count")
      assert length(timeline_result["count"]) == 2

      category_data = %{
        at: [~U[2025-08-17 10:00:00Z], ~U[2025-08-17 11:00:00Z]],
        values: [%{"0" => 5, "1" => 10}, %{"0" => 8, "1" => 12}]
      }

      # Test category formatter - needs a path parameter
      category_result = Trifle.Stats.Formatter.Category.format(category_data, "0")
      assert category_result == %{"0" => 13.0}
    end

    test "series functionality works" do
      # Test series creation
      data = %{values: [%{count: 10}, %{count: 20}, %{count: 30}]}
      series = Trifle.Stats.Series.new(data)

      assert %Trifle.Stats.Series{} = series
      assert is_map(series.series)
    end

    test "configuration functionality works" do
      # Test configuration creation
      config =
        Trifle.Stats.Configuration.configure(
          nil,
          time_zone: "UTC",
          time_zone_database: Tzdata.TimeZoneDatabase,
          beginning_of_week: :monday,
          validate_driver: false
        )

      assert %Trifle.Stats.Configuration{} = config
      assert config.time_zone == "UTC"
      assert config.time_zone_database == Tzdata.TimeZoneDatabase
      assert config.beginning_of_week == :monday

      # Test configuration updates
      updated_config = Trifle.Stats.Configuration.set_time_zone(config, "America/New_York")
      assert updated_config.time_zone == "America/New_York"
    end

    test "packer functionality works" do
      # Test data normalization
      data = [%{count: 5}, %{count: 10}]
      normalized = Trifle.Stats.Packer.normalize(data)

      assert is_list(normalized)

      # Test pack/unpack
      packed = Trifle.Stats.Packer.pack(%{test: "value"})
      assert is_map(packed)

      unpacked = Trifle.Stats.Packer.unpack(packed)
      assert is_map(unpacked)
    end

    test "transponder functionality works" do
      # Test data structure for transponders
      transponder_data = %{
        at: [~U[2025-08-17 10:00:00Z], ~U[2025-08-17 11:00:00Z]],
        values: [
          %{
            sum: 100,
            count: 10,
            conversions: 5,
            visits: 50,
            response_sum: 600,
            response_count: 3,
            response_square: 140_000
          },
          %{
            sum: 200,
            count: 20,
            conversions: 8,
            visits: 40,
            response_sum: 750,
            response_count: 3,
            response_square: 187_500
          }
        ]
      }

      # Test Divide transponder
      div_result =
        Trifle.Stats.Transponder.Divide.transform(transponder_data, "sum", "count", "average")

      assert is_map(div_result)
      [first_div, _] = div_result.values
      # 100/10
      assert first_div[:average] == 10.0

      # Test Ratio transponder
      ratio_result =
        Trifle.Stats.Transponder.Ratio.transform(
          transponder_data,
          "conversions",
          "visits",
          "conversion_rate"
        )

      assert is_map(ratio_result)
      [first_ratio, second_ratio] = ratio_result.values
      # 5/50 * 100
      assert first_ratio[:conversion_rate] == 10.0
      # 8/40 * 100
      assert second_ratio[:conversion_rate] == 20.0

      # Test Standard Deviation transponder  
      stddev_result =
        Trifle.Stats.Transponder.StandardDeviation.transform(
          transponder_data,
          "response_sum",
          "response_count",
          "response_square",
          "stddev"
        )

      assert is_map(stddev_result)
      [first_stddev, _] = stddev_result.values
      assert is_number(first_stddev[:stddev])
      assert first_stddev[:stddev] > 0

      # Test Series integration with transponders
      series = Trifle.Stats.Series.new(transponder_data)

      # New pipe-friendly API with separate arguments
      div_series = Trifle.Stats.Series.transform_divide(series, "sum", "count", "average")
      assert %Trifle.Stats.Series{} = div_series

      # Chained transformations
      final_series =
        series
        |> Trifle.Stats.Series.transform_ratio("conversions", "visits", "conversion_rate")

      assert %Trifle.Stats.Series{} = final_series
    end

    @tag :integration
    test "SQLite driver basic operations work" do
      # Create in-memory SQLite database
      {:ok, conn} = Exqlite.start_link(database: ":memory:")
      table_name = "test_comprehensive_sqlite"
      driver = Trifle.Stats.Driver.Sqlite.new(conn, table_name)

      # Setup tables
      assert :ok = Trifle.Stats.Driver.Sqlite.setup!(conn, table_name)

      # Test increment operation with new API
      keys = to_keys([["test_key", "hour", 1_692_266_400]])
      values = %{count: 5}
      Trifle.Stats.Driver.Sqlite.inc(keys, values, driver)

      # Test set operation with new API  
      set_keys = to_keys([["set_key", "hour", 1_692_266_400]])
      set_values = %{count: 10, status: "active"}
      Trifle.Stats.Driver.Sqlite.set(set_keys, set_values, driver)

      # Test get operation
      results = Trifle.Stats.Driver.Sqlite.get(set_keys, driver)
      result = Enum.at(results, 0)
      assert result["count"] == 10
      assert result["status"] == "active"

      # Test ping/scan operations in separated mode
      sep_table_name = "test_comp_sqlite_sep"
      ping_table_name = "#{sep_table_name}_ping"
      sep_driver = Trifle.Stats.Driver.Sqlite.new(conn, sep_table_name, ping_table_name, nil)
      assert :ok = Trifle.Stats.Driver.Sqlite.setup!(conn, sep_table_name, nil, ping_table_name)

      values = %{status: "active", count: 25}
      at = DateTime.utc_now()
      key = Trifle.Stats.Nocturnal.Key.new(key: "status_key", at: at)

      assert :ok = Trifle.Stats.Driver.Sqlite.ping(key, values, sep_driver)

      scan_key = Trifle.Stats.Nocturnal.Key.new(key: "status_key")
      [scan_at, scan_values] = Trifle.Stats.Driver.Sqlite.scan(scan_key, sep_driver)
      assert %{"status" => "active", "count" => 25} = scan_values["data"]
      assert %DateTime{} = scan_at

      # Clean up
      GenServer.stop(conn)
    end

    @tag :integration
    test "Process driver basic operations work" do
      # Start Process driver
      {:ok, pid} = Trifle.Stats.Driver.Process.start_link()
      driver = Trifle.Stats.Driver.Process.new(pid)

      # Test increment operation
      assert :ok =
               Trifle.Stats.Driver.Process.inc(
                 to_keys([["test_key", "hour", 1_692_266_400]]),
                 %{count: 5},
                 driver
               )

      # Test set operation  
      assert :ok =
               Trifle.Stats.Driver.Process.set(
                 to_keys([["set_key", "hour", 1_692_266_400]]),
                 %{count: 10},
                 driver
               )

      # Test get operation
      [result] =
        Trifle.Stats.Driver.Process.get(to_keys([["set_key", "hour", 1_692_266_400]]), driver)

      assert %{"count" => 10} = result

      # Test ping/scan operations
      values = %{status: "active", count: 25}
      at = DateTime.utc_now()
      key = Trifle.Stats.Nocturnal.Key.new(key: "status_key", at: at)

      assert :ok = Trifle.Stats.Driver.Process.ping(key, values, driver)

      scan_key = Trifle.Stats.Nocturnal.Key.new(key: "status_key")
      [scan_at, scan_values] = Trifle.Stats.Driver.Process.scan(scan_key, driver)
      assert %{"status" => "active", "count" => 25} = scan_values["data"]
      assert %DateTime{} = scan_at

      # Clean up
      GenServer.stop(pid)
    end
  end
end
