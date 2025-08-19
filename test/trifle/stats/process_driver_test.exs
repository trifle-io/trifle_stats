defmodule Trifle.Stats.ProcessDriverTest do
  use ExUnit.Case

  @moduletag :integration

  describe "Process driver" do
    test "basic operations work" do
      # Start the Process driver
      {:ok, pid} = Trifle.Stats.Driver.Process.start_link()
      driver = Trifle.Stats.Driver.Process.new(pid)
      
      # Test increment operation with Nocturnal::Key objects
      keys = [Trifle.Stats.Nocturnal.Key.new(key: "test_key", granularity: "hour", at: DateTime.from_unix!(1692266400))]
      values = %{count: 5}
      Trifle.Stats.Driver.Process.inc(keys, values, driver)
      
      # Test get operation
      [result] = Trifle.Stats.Driver.Process.get(keys, driver)
      assert %{"count" => 5} = result
      
      # Test increment again (should add)
      Trifle.Stats.Driver.Process.inc(keys, %{count: 3}, driver)
      [result2] = Trifle.Stats.Driver.Process.get(keys, driver)
      assert %{"count" => 8} = result2
      
      # Test set operation (should replace)
      Trifle.Stats.Driver.Process.set(keys, %{count: 10, status: "active"}, driver)
      [result3] = Trifle.Stats.Driver.Process.get(keys, driver)
      assert %{"count" => 10, "status" => "active"} = result3
      
      # Test ping/scan operations
      values = %{status: "running", count: 25}
      at = DateTime.utc_now()
      key = Trifle.Stats.Nocturnal.Key.new(key: "status_key", at: at)
      
      assert :ok = Trifle.Stats.Driver.Process.ping(key, values, driver)
      
      scan_key = Trifle.Stats.Nocturnal.Key.new(key: "status_key")
      [scan_at, scan_values] = Trifle.Stats.Driver.Process.scan(scan_key, driver)
      assert %{"status" => "running", "count" => 25} = scan_values["data"]
      assert %DateTime{} = scan_at
      
      # Test clear functionality
      Trifle.Stats.Driver.Process.clear(driver)
      [empty_result] = Trifle.Stats.Driver.Process.get(keys, driver)
      assert empty_result == %{}
      
      [nil_at, nil_values] = Trifle.Stats.Driver.Process.scan(scan_key, driver)
      assert nil_at == nil
      assert nil_values == %{}
      
      # Clean up
      GenServer.stop(pid)
    end
    
    test "multiple keys work correctly" do
      {:ok, pid} = Trifle.Stats.Driver.Process.start_link()
      driver = Trifle.Stats.Driver.Process.new(pid)
      
      # Test with multiple keys using Nocturnal::Key objects
      keys = [
        Trifle.Stats.Nocturnal.Key.new(key: "metric1", granularity: "hour", at: DateTime.from_unix!(1692266400)),
        Trifle.Stats.Nocturnal.Key.new(key: "metric2", granularity: "hour", at: DateTime.from_unix!(1692266400)),
        Trifle.Stats.Nocturnal.Key.new(key: "metric1", granularity: "day", at: DateTime.from_unix!(1692230400))
      ]
      
      values = %{count: 1, amount: 10}
      Trifle.Stats.Driver.Process.inc(keys, values, driver)
      
      results = Trifle.Stats.Driver.Process.get(keys, driver)
      assert length(results) == 3
      assert Enum.all?(results, fn result -> 
        result["count"] == 1 and result["amount"] == 10 
      end)
      
      # Test that keys are independent
      Trifle.Stats.Driver.Process.inc([Enum.at(keys, 0)], %{count: 5}, driver)
      
      updated_results = Trifle.Stats.Driver.Process.get(keys, driver)
      assert Enum.at(updated_results, 0)["count"] == 6  # 1 + 5
      assert Enum.at(updated_results, 1)["count"] == 1  # unchanged
      assert Enum.at(updated_results, 2)["count"] == 1  # unchanged
      
      GenServer.stop(pid)
    end
    
    test "description method works" do
      {:ok, pid} = Trifle.Stats.Driver.Process.start_link()
      driver = Trifle.Stats.Driver.Process.new(pid)
      
      description = Trifle.Stats.Driver.Process.description(driver)
      assert String.contains?(description, "Trifle.Stats.Driver.Process")
      assert String.contains?(description, "PID")
      
      GenServer.stop(pid)
    end
    
    test "debug state method works" do
      {:ok, pid} = Trifle.Stats.Driver.Process.start_link()
      driver = Trifle.Stats.Driver.Process.new(pid)
      
      # Initially empty
      state = Trifle.Stats.Driver.Process.debug_state(driver)
      assert state.data == %{}
      assert state.status == %{}
      
      # Add some data
      keys = [Trifle.Stats.Nocturnal.Key.new(key: "test", granularity: "hour", at: DateTime.from_unix!(1692266400))]
      Trifle.Stats.Driver.Process.inc(keys, %{count: 5}, driver)
      
      # Ping some status
      key = Trifle.Stats.Nocturnal.Key.new(key: "status", at: DateTime.utc_now())
      Trifle.Stats.Driver.Process.ping(key, %{active: true}, driver)
      
      # Check state
      updated_state = Trifle.Stats.Driver.Process.debug_state(driver)
      assert map_size(updated_state.data) == 1
      assert map_size(updated_state.status) == 1
      
      GenServer.stop(pid)
    end
  end
end