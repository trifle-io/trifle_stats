defmodule Trifle.Stats.BufferTest do
  use ExUnit.Case, async: true

  alias Trifle.Stats.Buffer
  alias Trifle.Stats.Driver.Process, as: ProcessDriver
  alias Trifle.Stats.Nocturnal.Key

  setup do
    {:ok, pid} = ProcessDriver.start_link()
    driver = ProcessDriver.new(pid)
    key = Key.new(key: "metric", granularity: "1h", at: ~U[2023-01-01 12:00:00Z])

    %{driver: driver, key: key}
  end

  test "flushes when queue reaches configured size (non-aggregated)", %{driver: driver, key: key} do
    buffer = Buffer.new(driver: driver, duration: 60, size: 2, aggregate: false, async: false)

    Buffer.inc([key], %{count: 1}, buffer)
    Buffer.inc([key], %{count: 2}, buffer)

    Process.sleep(20)
    [values] = ProcessDriver.get([key], driver)
    assert values["count"] == 3

    Buffer.shutdown(buffer)
  end

  test "aggregates repeated increments for identical keys", %{driver: driver, key: key} do
    buffer = Buffer.new(driver: driver, duration: 60, size: 2, aggregate: true, async: false)

    Buffer.inc([key], %{count: 1, nested: %{requests: 1}}, buffer)
    Buffer.inc([key], %{count: 2, nested: %{requests: 3}}, buffer)
    Buffer.inc([key], %{count: 3, nested: %{requests: 5}}, buffer)
    Buffer.inc([key], %{count: 4, nested: %{requests: 7}}, buffer)

    Process.sleep(20)
    [values] = ProcessDriver.get([key], driver)
    assert values["count"] == 10
    assert values["nested"]["requests"] == 16

    Buffer.shutdown(buffer)
  end

  test "keeps last set operation when aggregating", %{driver: driver, key: key} do
    buffer = Buffer.new(driver: driver, duration: 60, size: 10, aggregate: true, async: false)

    Buffer.set([key], %{state: "processing"}, buffer)
    Buffer.set([key], %{state: "done", detail: %{attempts: 3}}, buffer)
    Buffer.flush(buffer)

    [values] = ProcessDriver.get([key], driver)
    assert values["state"] == "done"
    assert values["detail"]["attempts"] == 3

    Buffer.shutdown(buffer)
  end

  test "flushes automatically after duration expires", %{driver: driver, key: key} do
    buffer = Buffer.new(driver: driver, duration: 0.05, size: 10, aggregate: false, async: true)

    Buffer.inc([key], %{count: 5}, buffer)
    Process.sleep(100)

    [values] = ProcessDriver.get([key], driver)
    assert values["count"] == 5

    Buffer.shutdown(buffer)
  end

  test "flushes outstanding operations on shutdown", %{driver: driver, key: key} do
    buffer = Buffer.new(driver: driver, duration: 60, size: 10, aggregate: false, async: false)

    Buffer.inc([key], %{count: 7}, buffer)
    Buffer.shutdown(buffer)

    [values] = ProcessDriver.get([key], driver)
    assert values["count"] == 7
  end
end
