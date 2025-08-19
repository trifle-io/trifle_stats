defmodule Trifle.Stats.RedisDriverTest do
  use ExUnit.Case

  @moduletag :integration
  @moduletag :redis

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

  describe "Redis driver - joined identifier mode" do
    test "basic operations work" do
      # Skip if Redis is not available
      if not redis_available?() do
        IO.puts("Skipping Redis tests - Redis not available")
      else
      
      {:ok, conn} = Redix.start_link(host: "localhost", port: 6379)
      prefix = "test_redis_#{:rand.uniform(1000000)}"
      
      driver = Trifle.Stats.Driver.Redis.new(conn, prefix, "::")
      
      # Test increment operation  
      keys = to_keys([["page_views", "hour", 1692266400], ["page_views", "day", 1692230400]])
      values = %{count: 5, views: 100}
      Trifle.Stats.Driver.Redis.inc(keys, values, driver)
      
      # Test get operation
      results = Trifle.Stats.Driver.Redis.get(keys, driver)
      assert length(results) == 2
      assert Enum.all?(results, fn result -> 
        result["count"] == 5 and result["views"] == 100
      end)
      
      # Test increment again (should add)
      Trifle.Stats.Driver.Redis.inc(keys, %{count: 3, views: 50}, driver)
      updated_results = Trifle.Stats.Driver.Redis.get(keys, driver)
      assert Enum.all?(updated_results, fn result -> 
        result["count"] == 8 and result["views"] == 150
      end)
      
      # Test set operation (should replace)
      Trifle.Stats.Driver.Redis.set(keys, %{count: 20, status: "active"}, driver)
      set_results = Trifle.Stats.Driver.Redis.get(keys, driver)
      assert Enum.all?(set_results, fn result -> 
        result["count"] == 20 and result["status"] == "active" and result["views"] == nil
      end)
      
      # Test with empty keys
      empty_results = Trifle.Stats.Driver.Redis.get(to_keys([["non_existent", "hour", 1692266400]]), driver)
      assert [%{}] = empty_results
      
      # Clean up - delete test keys
      keys_to_delete = [
        "#{prefix}::page_views::hour::1692266400",
        "#{prefix}::page_views::day::1692230400"
      ]
      Enum.each(keys_to_delete, fn key ->
        Redix.command(conn, ["DEL", key])
      end)
      
        GenServer.stop(conn)
      end
    end
    
    test "ping/scan operations work (stub implementations)" do
      if not redis_available?() do
        IO.puts("Skipping Redis tests - Redis not available")
      else
      
      {:ok, conn} = Redix.start_link(host: "localhost", port: 6379)
      prefix = "test_redis_ping_#{:rand.uniform(1000000)}"
      
      driver = Trifle.Stats.Driver.Redis.new(conn, prefix)
      
      # Test ping operation (should succeed but not store anything)
      values = %{status: "running", count: 25}
      at = DateTime.utc_now()
      key = Trifle.Stats.Nocturnal.Key.new(key: "status_key", at: at)
      
      assert :ok = Trifle.Stats.Driver.Redis.ping(key, values, driver)
      
      # Test scan operation (should return empty)
      scan_key = Trifle.Stats.Nocturnal.Key.new(key: "status_key")
      assert [] = Trifle.Stats.Driver.Redis.scan(scan_key, driver)
      
        GenServer.stop(conn)
      end
    end
    
    test "multiple keys work independently" do
      if not redis_available?() do
        IO.puts("Skipping Redis tests - Redis not available")
      else
      
      {:ok, conn} = Redix.start_link(host: "localhost", port: 6379)
      prefix = "test_redis_multi_#{:rand.uniform(1000000)}"
      
      driver = Trifle.Stats.Driver.Redis.new(conn, prefix)
      
      # Test multiple independent keys
      keys = to_keys([
        ["metric_a", "hour", 1692266400],
        ["metric_b", "hour", 1692266400], 
        ["metric_a", "day", 1692230400]
      ])
      
      values = %{count: 1, amount: 10}
      Trifle.Stats.Driver.Redis.inc(keys, values, driver)
      
      # Increment only the first key
      Trifle.Stats.Driver.Redis.inc([Enum.at(keys, 0)], %{count: 5, amount: 20}, driver)
      
      results = Trifle.Stats.Driver.Redis.get(keys, driver)
      assert Enum.at(results, 0)["count"] == 6   # 1 + 5
      assert Enum.at(results, 0)["amount"] == 30 # 10 + 20
      assert Enum.at(results, 1)["count"] == 1   # unchanged
      assert Enum.at(results, 1)["amount"] == 10 # unchanged 
      assert Enum.at(results, 2)["count"] == 1   # unchanged
      assert Enum.at(results, 2)["amount"] == 10 # unchanged
      
      # Clean up
      keys_to_delete = [
        "#{prefix}::metric_a::hour::1692266400",
        "#{prefix}::metric_b::hour::1692266400",
        "#{prefix}::metric_a::day::1692230400"
      ]
      Enum.each(keys_to_delete, fn key ->
        Redix.command(conn, ["DEL", key])
      end)
      
        GenServer.stop(conn)
      end
    end
    
    test "different separators work correctly" do
      if not redis_available?() do
        IO.puts("Skipping Redis tests - Redis not available")
      else
      
      {:ok, conn} = Redix.start_link(host: "localhost", port: 6379)
      prefix = "test_redis_sep_#{:rand.uniform(1000000)}"
      
      # Test with different separator
      driver = Trifle.Stats.Driver.Redis.new(conn, prefix, ":")
      
      keys = to_keys([["test", "hour", 1692266400]])
      values = %{count: 42}
      Trifle.Stats.Driver.Redis.inc(keys, values, driver)
      
      results = Trifle.Stats.Driver.Redis.get(keys, driver)
      assert [%{"count" => 42}] = results
      
      # Verify the key format by checking Redis directly
      expected_key = "#{prefix}:test:hour:1692266400"
      {:ok, exists} = Redix.command(conn, ["EXISTS", expected_key])
      assert exists == 1
      
      # Clean up
      Redix.command(conn, ["DEL", expected_key])
        GenServer.stop(conn)
      end
    end
    
    test "payload conversion works correctly" do
      if not redis_available?() do
        IO.puts("Skipping Redis tests - Redis not available")
      else
      
      # Test payload_to_map function
      payload = ["field1", "value1", "field2", "value2", "field3", "value3"]
      expected_map = %{"field1" => "value1", "field2" => "value2", "field3" => "value3"}
      assert Trifle.Stats.Driver.Redis.payload_to_map(payload) == expected_map
      
      # Test map_to_payload function
      map = %{"a" => 1, "b" => 2, "c" => 3}
      result_payload = Trifle.Stats.Driver.Redis.map_to_payload(map)
      
      # Since map order isn't guaranteed, convert back to map for comparison
      result_map = Trifle.Stats.Driver.Redis.payload_to_map(result_payload)
      expected_result = %{"a" => 1, "b" => 2, "c" => 3}
      assert result_map == expected_result
      end
    end
  end

  describe "Redis driver - separated key components" do
    test "handles multi-component keys correctly" do
      if not redis_available?() do
        IO.puts("Skipping Redis tests - Redis not available")
      else
        {:ok, conn} = Redix.start_link(host: "localhost", port: 6379)
        prefix = "test_redis_separated_#{:rand.uniform(1000000)}"
        
        driver = Trifle.Stats.Driver.Redis.new(conn, prefix, "::")
        
        # Test keys with multiple components (similar to separated mode)
        keys = to_keys([
          ["user_events", "hour", "1692266400"],
          ["user_events", "day", "1692230400"],
          ["page_views", "hour", "1692266400"]
        ])
        
        values = %{clicks: 10, views: 50}
        Trifle.Stats.Driver.Redis.inc(keys, values, driver)
        
        # Test get operation
        results = Trifle.Stats.Driver.Redis.get(keys, driver)
        assert length(results) == 3
        assert Enum.all?(results, fn result -> 
          result["clicks"] == 10 and result["views"] == 50
        end)
        
        # Test increment again (should add)
        Trifle.Stats.Driver.Redis.inc(keys, %{clicks: 5, views: 25}, driver)
        updated_results = Trifle.Stats.Driver.Redis.get(keys, driver)
        assert Enum.all?(updated_results, fn result -> 
          result["clicks"] == 15 and result["views"] == 75
        end)
        
        # Test set operation (should replace)
        Trifle.Stats.Driver.Redis.set(keys, %{clicks: 100, status: "processed"}, driver)
        set_results = Trifle.Stats.Driver.Redis.get(keys, driver)
        assert Enum.all?(set_results, fn result -> 
          result["clicks"] == 100 and result["status"] == "processed" and result["views"] == nil
        end)
        
        # Verify keys are properly formatted in Redis
        expected_keys = [
          "#{prefix}::user_events::hour::1692266400",
          "#{prefix}::user_events::day::1692230400", 
          "#{prefix}::page_views::hour::1692266400"
        ]
        
        # Check that keys exist in Redis
        Enum.each(expected_keys, fn key ->
          {:ok, exists} = Redix.command(conn, ["EXISTS", key])
          assert exists == 1
        end)
        
        # Clean up - delete test keys
        Enum.each(expected_keys, fn key ->
          Redix.command(conn, ["DEL", key])
        end)
        
        GenServer.stop(conn)
      end
    end
    
    test "different separator creates different key structure" do
      if not redis_available?() do
        IO.puts("Skipping Redis tests - Redis not available")
      else
        {:ok, conn} = Redix.start_link(host: "localhost", port: 6379)
        prefix = "test_redis_sep_structure_#{:rand.uniform(1000000)}"
        
        # Test with single-character separator
        driver = Trifle.Stats.Driver.Redis.new(conn, prefix, ":")
        
        keys = to_keys([["metric", "hour", "123456789"]])
        values = %{count: 42}
        Trifle.Stats.Driver.Redis.inc(keys, values, driver)
        
        # Verify the key format
        expected_key = "#{prefix}:metric:hour:123456789"
        {:ok, exists} = Redix.command(conn, ["EXISTS", expected_key])
        assert exists == 1
        
        # Clean up
        Redix.command(conn, ["DEL", expected_key])
        GenServer.stop(conn)
      end
    end
  end
  
  # Helper function to check if Redis is available
  defp redis_available? do
    try do
      {:ok, pid} = Redix.start_link(host: "localhost", port: 6379, timeout: 1000)
      GenServer.stop(pid)
      true
    rescue
      _ -> false
    catch
      :exit, _ -> false
    end
  end
end