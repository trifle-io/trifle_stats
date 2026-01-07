defmodule Trifle.Stats.MongoDriverTest do
  use ExUnit.Case

  @moduletag :integration
  @moduletag :mongo
  
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

  describe "MongoDB driver - joined identifier mode" do
    test "basic operations work" do
      # Skip if MongoDB is not available
      if not mongo_available?() do
        IO.puts("Skipping MongoDB tests - MongoDB not available")
      else
      
      {:ok, conn} = Mongo.start_link(url: "mongodb://localhost:27017/test_trifle_stats")
      collection_name = "test_joined_#{:rand.uniform(1000000)}"
      
      driver = Trifle.Stats.Driver.Mongo.new(conn, collection_name, "::", 1, :full)
      
      # Setup collection
      assert :ok = Trifle.Stats.Driver.Mongo.setup!(conn, collection_name)
      
      # Test increment operation  
      keys = to_keys([["page_views", "hour", 1692266400], ["page_views", "day", 1692230400]])
      values = %{count: 5, amount: 100}
      Trifle.Stats.Driver.Mongo.inc(keys, values, driver)
      
      # Test get operation
      results = Trifle.Stats.Driver.Mongo.get(keys, driver)
      assert length(results) == 2
      assert Enum.all?(results, fn result -> 
        result["count"] == 5 and result["amount"] == 100 
      end)
      
      # Test increment again (should add)
      Trifle.Stats.Driver.Mongo.inc(keys, %{count: 3, amount: 50}, driver)
      updated_results = Trifle.Stats.Driver.Mongo.get(keys, driver)
      assert Enum.all?(updated_results, fn result -> 
        result["count"] == 8 and result["amount"] == 150 
      end)
      
      # Test set operation (should replace)
      Trifle.Stats.Driver.Mongo.set(keys, %{count: 20, status: "active"}, driver)
      set_results = Trifle.Stats.Driver.Mongo.get(keys, driver)
      assert Enum.all?(set_results, fn result -> 
        result["count"] == 20 and result["status"] == "active" and result["amount"] == nil
      end)
      
      # Test ping/scan operations (should return empty in joined mode)
      values = %{status: "running", count: 25}
      at = DateTime.utc_now()
      key = Trifle.Stats.Nocturnal.Key.new(key: "status_key", at: at)
      
      assert [] = Trifle.Stats.Driver.Mongo.ping(key, values, driver)
      
      scan_key = Trifle.Stats.Nocturnal.Key.new(key: "status_key")
      assert [] = Trifle.Stats.Driver.Mongo.scan(scan_key, driver)
      
        # Clean up
        Mongo.drop_collection(conn, collection_name)
        GenServer.stop(conn)
      end
    end
  end

  describe "MongoDB driver - separated identifier mode" do
    test "basic operations work" do
      if not mongo_available?() do
        IO.puts("Skipping MongoDB tests - MongoDB not available")
      else
      
      {:ok, conn} = Mongo.start_link(url: "mongodb://localhost:27017/test_trifle_stats")
      collection_name = "test_separated_#{:rand.uniform(1000000)}"
      
      driver = Trifle.Stats.Driver.Mongo.new(conn, collection_name, nil, 1, nil)
      
      # Setup collection  
      assert :ok = Trifle.Stats.Driver.Mongo.setup!(conn, collection_name)
      
      # Test increment operation
      keys = to_keys([["user_events", "hour", 1692266400], ["user_events", "day", 1692230400]])
      values = %{clicks: 10, views: 50}
      Trifle.Stats.Driver.Mongo.inc(keys, values, driver)
      
      # Test get operation
      results = Trifle.Stats.Driver.Mongo.get(keys, driver)
      assert length(results) == 2
      assert Enum.all?(results, fn result -> 
        result["clicks"] == 10 and result["views"] == 50
      end)
      
      # Test set operation
      Trifle.Stats.Driver.Mongo.set(keys, %{clicks: 100, status: "processed"}, driver)
      set_results = Trifle.Stats.Driver.Mongo.get(keys, driver)
      assert Enum.all?(set_results, fn result -> 
        result["clicks"] == 100 and result["status"] == "processed" and result["views"] == nil
      end)
      
        # Clean up
        Mongo.drop_collection(conn, collection_name)
        GenServer.stop(conn)
      end
    end
    
    test "ping/scan operations work in separated mode" do
      if not mongo_available?() do
        IO.puts("Skipping MongoDB tests - MongoDB not available")
      else
      
      {:ok, conn} = Mongo.start_link(url: "mongodb://localhost:27017/test_trifle_stats")
      collection_name = "test_ping_scan_#{:rand.uniform(1000000)}"
      
      driver = Trifle.Stats.Driver.Mongo.new(conn, collection_name, nil, 1, nil)
      
      # Setup collection for separated mode
      assert :ok = Trifle.Stats.Driver.Mongo.setup!(conn, collection_name, nil)
      
      # Test ping operation
      values = %{status: "running", count: 25, temperature: 78.5}
      at = DateTime.utc_now()
      key = Trifle.Stats.Nocturnal.Key.new(key: "server_status", at: at)
      
      assert :ok = Trifle.Stats.Driver.Mongo.ping(key, values, driver)
      
      # Test scan operation
      scan_key = Trifle.Stats.Nocturnal.Key.new(key: "server_status")
      [scan_at, scan_values] = Trifle.Stats.Driver.Mongo.scan(scan_key, driver)
      
      assert %DateTime{} = scan_at
      assert %{"status" => "running", "count" => 25, "temperature" => 78.5} = scan_values["data"]
      
      # Test ping again with new data (should update)
      new_values = %{status: "idle", count: 10}
      new_at = DateTime.add(at, 60, :second)
      new_key = Trifle.Stats.Nocturnal.Key.new(key: "server_status", at: new_at)
      
      assert :ok = Trifle.Stats.Driver.Mongo.ping(new_key, new_values, driver)
      
      # Scan should return the latest data
      [latest_at, latest_values] = Trifle.Stats.Driver.Mongo.scan(scan_key, driver)
      assert DateTime.compare(latest_at, scan_at) == :gt
      assert %{"status" => "idle", "count" => 10} = latest_values["data"]
      
      # Test scan for non-existent key
      missing_key = Trifle.Stats.Nocturnal.Key.new(key: "non_existent")
      assert [] = Trifle.Stats.Driver.Mongo.scan(missing_key, driver)
      
        # Clean up
        Mongo.drop_collection(conn, collection_name)
        GenServer.stop(conn)
      end
    end
    
    test "multiple keys work independently" do
      if not mongo_available?() do
        IO.puts("Skipping MongoDB tests - MongoDB not available")
      else
      
      {:ok, conn} = Mongo.start_link(url: "mongodb://localhost:27017/test_trifle_stats")
      collection_name = "test_multi_keys_#{:rand.uniform(1000000)}"
      
      driver = Trifle.Stats.Driver.Mongo.new(conn, collection_name, nil, 1, nil)
      
      # Setup collection
      assert :ok = Trifle.Stats.Driver.Mongo.setup!(conn, collection_name, nil)
      
      # Test multiple independent keys
      keys = to_keys([
        ["metric_a", "hour", 1692266400],
        ["metric_b", "hour", 1692266400],
        ["metric_a", "day", 1692230400]
      ])
      
      values = %{count: 1}
      Trifle.Stats.Driver.Mongo.inc(keys, values, driver)
      
      # Increment only the first key
      Trifle.Stats.Driver.Mongo.inc([Enum.at(keys, 0)], %{count: 5}, driver)
      
      results = Trifle.Stats.Driver.Mongo.get(keys, driver)
      assert Enum.at(results, 0)["count"] == 6  # 1 + 5
      assert Enum.at(results, 1)["count"] == 1  # unchanged
      assert Enum.at(results, 2)["count"] == 1  # unchanged
      
        # Clean up
        Mongo.drop_collection(conn, collection_name)
        GenServer.stop(conn)
      end
    end
  end
  
  # Helper function to check if MongoDB is available
  defp mongo_available? do
    try do
      {:ok, pid} = Mongo.start_link(url: "mongodb://localhost:27017/test", timeout: 1000)
      GenServer.stop(pid)
      true
    rescue
      _ -> false
    catch
      :exit, _ -> false
    end
  end
end
