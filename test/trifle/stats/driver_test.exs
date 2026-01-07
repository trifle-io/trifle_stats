defmodule Trifle.Stats.DriverTest do
  use ExUnit.Case

  @moduletag :integration
  
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
      # Create in-memory SQLite database
      {:ok, conn} = Exqlite.start_link(database: ":memory:")
      table_name = "test_sqlite_driver"
      driver = Trifle.Stats.Driver.Sqlite.new(conn, table_name)
      
      # Setup tables
      assert :ok = Trifle.Stats.Driver.Sqlite.setup!(conn, table_name)
      
      # Test increment operation with new API
      keys = to_keys([["test_key", "hour", 1692266400]])
      values = %{count: 5}
      Trifle.Stats.Driver.Sqlite.inc(keys, values, driver)
      
      # Test set operation with new API  
      set_keys = to_keys([["set_key", "hour", 1692266400]])
      set_values = %{count: 10, status: "active"}
      Trifle.Stats.Driver.Sqlite.set(set_keys, set_values, driver)
      
      # Test get operation
      results = Trifle.Stats.Driver.Sqlite.get(set_keys, driver)
      result = Enum.at(results, 0)
      assert result["count"] == 10
      assert result["status"] == "active"
      
      # Test ping/scan operations in separated mode
      sep_table_name = "test_sqlite_driver_sep"
      ping_table_name = "#{sep_table_name}_ping"
      sep_driver = Trifle.Stats.Driver.Sqlite.new(conn, sep_table_name, ping_table_name, nil)
      assert :ok = Trifle.Stats.Driver.Sqlite.setup!(conn, sep_table_name, nil, ping_table_name)
      
      ping_values = %{status: "running", count: 25}
      at = DateTime.utc_now()
      key = Trifle.Stats.Nocturnal.Key.new(key: "status_key", at: at)
      
      assert :ok = Trifle.Stats.Driver.Sqlite.ping(key, ping_values, sep_driver)
      
      scan_key = Trifle.Stats.Nocturnal.Key.new(key: "status_key")
      [scan_at, scan_values] = Trifle.Stats.Driver.Sqlite.scan(scan_key, sep_driver)
      assert scan_values["data"]["status"] == "running"
      assert scan_values["data"]["count"] == 25
      assert %DateTime{} = scan_at
      
      # Clean up
      GenServer.stop(conn)
    end
  end
end
