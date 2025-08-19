defmodule Trifle.Stats.SqliteDriverTest do
  use ExUnit.Case

  @moduletag :integration
  @moduletag :sqlite
  
  # Helper function to convert array keys to Nocturnal::Key objects
  defp to_key([key_name, granularity, timestamp]) when is_binary(timestamp) do
    Trifle.Stats.Nocturnal.Key.new(key: key_name, granularity: granularity, at: DateTime.from_unix!(String.to_integer(timestamp)))
  end
  
  defp to_key([key_name, granularity, timestamp]) when is_integer(timestamp) do
    Trifle.Stats.Nocturnal.Key.new(key: key_name, granularity: granularity, at: DateTime.from_unix!(timestamp))
  end
  
  defp to_keys(array_keys) do
    Enum.map(array_keys, &to_key/1)
  end

  describe "SQLite driver" do
    test "basic operations work" do
      # Create in-memory SQLite database for testing
      {:ok, conn} = Exqlite.start_link(database: ":memory:")
      table_name = "test_sqlite_#{:rand.uniform(1000000)}"
      
      driver = Trifle.Stats.Driver.Sqlite.new(conn, table_name)
      
      # Setup table
      assert :ok = Trifle.Stats.Driver.Sqlite.setup!(conn, table_name)
      
      # Test increment operation
      keys = to_keys([["page_views", "hour", 1692266400], ["page_views", "day", 1692230400]])
      values = %{count: 5, amount: 100}
      Trifle.Stats.Driver.Sqlite.inc(keys, values, driver)
      
      # Test get operation
      results = Trifle.Stats.Driver.Sqlite.get(keys, driver)
      assert length(results) == 2
      assert Enum.all?(results, fn result -> 
        result["count"] == 5 and result["amount"] == 100 
      end)
      
      # Test increment again (should add)
      Trifle.Stats.Driver.Sqlite.inc(keys, %{count: 3, amount: 50}, driver)
      updated_results = Trifle.Stats.Driver.Sqlite.get(keys, driver)
      assert Enum.all?(updated_results, fn result -> 
        result["count"] == 8 and result["amount"] == 150 
      end)
      
      # Test set operation (should replace)
      Trifle.Stats.Driver.Sqlite.set(keys, %{count: 20, status: "active"}, driver)
      set_results = Trifle.Stats.Driver.Sqlite.get(keys, driver)
      assert Enum.all?(set_results, fn result -> 
        result["count"] == 20 and result["status"] == "active" and result["amount"] == nil
      end)
      
      # Test with empty keys
      empty_results = Trifle.Stats.Driver.Sqlite.get(to_keys([["non_existent", "hour", 1692266400]]), driver)
      assert [%{}] = empty_results
      
      # Clean up
      GenServer.stop(conn)
    end
    
    test "ping/scan operations work" do
      {:ok, conn} = Exqlite.start_link(database: ":memory:")
      table_name = "test_sqlite_ping_#{:rand.uniform(1000000)}"
      ping_table_name = "#{table_name}_ping"
      
      driver = Trifle.Stats.Driver.Sqlite.new(conn, table_name, ping_table_name, false)
      
      # Setup tables for separated mode
      assert :ok = Trifle.Stats.Driver.Sqlite.setup!(conn, table_name, false, ping_table_name)
      assert :ok = Trifle.Stats.Driver.Sqlite.setup_ping_table!(conn, ping_table_name)
      
      # Test ping operation
      values = %{status: "running", count: 25, temperature: 78.5}
      at = DateTime.utc_now()
      key = Trifle.Stats.Nocturnal.Key.new(key: "server_status", at: at)
      
      assert :ok = Trifle.Stats.Driver.Sqlite.ping(key, values, driver)
      
      # Test scan operation
      scan_key = Trifle.Stats.Nocturnal.Key.new(key: "server_status")
      [scan_at, scan_values] = Trifle.Stats.Driver.Sqlite.scan(scan_key, driver)
      
      assert %DateTime{} = scan_at
      assert %{"status" => "running", "count" => 25, "temperature" => 78.5} = scan_values["data"]
      
      # Test ping again with new data (should update)
      new_values = %{status: "idle", count: 10}
      new_at = DateTime.add(at, 60, :second)
      new_key = Trifle.Stats.Nocturnal.Key.new(key: "server_status", at: new_at)
      
      assert :ok = Trifle.Stats.Driver.Sqlite.ping(new_key, new_values, driver)
      
      # Scan should return the latest data
      [latest_at, latest_values] = Trifle.Stats.Driver.Sqlite.scan(scan_key, driver)
      assert DateTime.compare(latest_at, scan_at) == :gt
      assert %{"status" => "idle", "count" => 10} = latest_values["data"]
      
      # Test scan for non-existent key
      missing_key = Trifle.Stats.Nocturnal.Key.new(key: "non_existent")
      assert [] = Trifle.Stats.Driver.Sqlite.scan(missing_key, driver)
      
      # Clean up
      GenServer.stop(conn)
    end
    
    test "multiple keys work independently" do
      {:ok, conn} = Exqlite.start_link(database: ":memory:")
      table_name = "test_sqlite_multi_#{:rand.uniform(1000000)}"
      
      driver = Trifle.Stats.Driver.Sqlite.new(conn, table_name)
      
      # Setup table
      assert :ok = Trifle.Stats.Driver.Sqlite.setup!(conn, table_name)
      
      # Test multiple independent keys
      keys = to_keys([
        ["metric_a", "hour", 1692266400],
        ["metric_b", "hour", 1692266400], 
        ["metric_a", "day", 1692230400]
      ])
      
      values = %{count: 1, amount: 10}
      Trifle.Stats.Driver.Sqlite.inc(keys, values, driver)
      
      # Increment only the first key
      Trifle.Stats.Driver.Sqlite.inc([Enum.at(keys, 0)], %{count: 5, amount: 20}, driver)
      
      results = Trifle.Stats.Driver.Sqlite.get(keys, driver)
      assert Enum.at(results, 0)["count"] == 6   # 1 + 5
      assert Enum.at(results, 0)["amount"] == 30 # 10 + 20
      assert Enum.at(results, 1)["count"] == 1   # unchanged
      assert Enum.at(results, 1)["amount"] == 10 # unchanged 
      assert Enum.at(results, 2)["count"] == 1   # unchanged
      assert Enum.at(results, 2)["amount"] == 10 # unchanged
      
      # Clean up
      GenServer.stop(conn)
    end
    
    test "JSON operations work correctly" do
      {:ok, conn} = Exqlite.start_link(database: ":memory:")
      table_name = "test_sqlite_json_#{:rand.uniform(1000000)}"
      
      driver = Trifle.Stats.Driver.Sqlite.new(conn, table_name)
      
      # Setup table
      assert :ok = Trifle.Stats.Driver.Sqlite.setup!(conn, table_name)
      
      # Test complex nested data
      keys = to_keys([["complex_data", "hour", 1692266400]])
      values = %{
        count: 42,
        nested: %{
          deep: %{
            value: 100
          }
        },
        array: [1, 2, 3],
        mixed: %{
          number: 123,
          string: "test",
          boolean: true
        }
      }
      
      Trifle.Stats.Driver.Sqlite.set(keys, values, driver)
      
      results = Trifle.Stats.Driver.Sqlite.get(keys, driver)
      result = Enum.at(results, 0)
      
      assert result["count"] == 42
      assert result["nested"]["deep"]["value"] == 100
      assert result["array"] == [1, 2, 3]
      assert result["mixed"]["number"] == 123
      assert result["mixed"]["string"] == "test"
      assert result["mixed"]["boolean"] == true
      
      # Test increment on nested values
      Trifle.Stats.Driver.Sqlite.inc(keys, %{count: 8, "nested.deep.value": 50}, driver)
      inc_results = Trifle.Stats.Driver.Sqlite.get(keys, driver)
      inc_result = Enum.at(inc_results, 0)
      
      assert inc_result["count"] == 50         # 42 + 8
      assert inc_result["nested"]["deep"]["value"] == 150  # 100 + 50
      
      # Clean up
      GenServer.stop(conn)
    end
    
    test "separated identifier mode operations" do
      {:ok, conn} = Exqlite.start_link(database: ":memory:")
      table_name = "test_sqlite_separated_#{:rand.uniform(1000000)}"
      
      driver = Trifle.Stats.Driver.Sqlite.new(conn, table_name, nil, false)
      
      # Setup table for separated mode
      assert :ok = Trifle.Stats.Driver.Sqlite.setup!(conn, table_name, false)
      
      # Test operations with separated identifiers
      keys = to_keys([["user_events", "hour", 1692266400], ["user_events", "day", 1692230400]])
      values = %{clicks: 10, views: 50}
      Trifle.Stats.Driver.Sqlite.inc(keys, values, driver)
      
      # Test get operation
      results = Trifle.Stats.Driver.Sqlite.get(keys, driver)
      assert length(results) == 2
      assert Enum.all?(results, fn result -> 
        result["clicks"] == 10 and result["views"] == 50
      end)
      
      # Test set operation
      Trifle.Stats.Driver.Sqlite.set(keys, %{clicks: 100, status: "processed"}, driver)
      set_results = Trifle.Stats.Driver.Sqlite.get(keys, driver)
      assert Enum.all?(set_results, fn result -> 
        result["clicks"] == 100 and result["status"] == "processed" and result["views"] == nil
      end)
      
      # Clean up
      GenServer.stop(conn)
    end
  end
end