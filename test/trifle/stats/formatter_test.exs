defmodule Trifle.Stats.FormatterTest do
  use ExUnit.Case

  describe "formatter functions" do
    test "category formatter aggregates single path" do
      data = %{
        at: [~U[2025-08-17 10:00:00Z], ~U[2025-08-17 11:00:00Z]],
        values: [
          %{"breakdown" => %{"success" => 5, "failure" => 3}},
          %{"breakdown" => %{"success" => 8, "failure" => 2}}
        ]
      }

      result = Trifle.Stats.Formatter.Category.format(data, "breakdown.success")

      assert %{"breakdown.success" => value} = result
      assert value == 13.0
    end

    test "category formatter expands wildcards" do
      data = %{
        at: [~U[2025-08-17 10:00:00Z], ~U[2025-08-17 11:00:00Z]],
        values: [
          %{"breakdown" => %{"success" => 5, "failure" => 3}},
          %{"breakdown" => %{"success" => 8, "failure" => 2}}
        ]
      }

      result = Trifle.Stats.Formatter.Category.format(data, "breakdown.*")

      assert result == %{
        "breakdown.failure" => 5.0,
        "breakdown.success" => 13.0
      }
    end

    test "timeline formatter returns map for single path" do
      data = %{
        at: [~U[2025-08-17 10:00:00Z], ~U[2025-08-17 11:00:00Z]],
        values: [%{count: 5}, %{count: 10}]
      }

      result = Trifle.Stats.Formatter.Timeline.format(data, "count")

      assert %{"count" => entries} = result
      assert length(entries) == 2

      [first, second] = entries
      assert %{at: ~U[2025-08-17 10:00:00Z], value: 5.0} = first
      assert %{at: ~U[2025-08-17 11:00:00Z], value: 10.0} = second
    end

    test "timeline formatter expands wildcards" do
      data = %{
        at: [~U[2025-08-17 10:00:00Z], ~U[2025-08-17 11:00:00Z]],
        values: [
          %{"breakdown" => %{"success" => 5, "failure" => 3}},
          %{"breakdown" => %{"success" => 8, "failure" => 2}}
        ]
      }

      result = Trifle.Stats.Formatter.Timeline.format(data, "breakdown.*")

      assert Map.keys(result) |> Enum.sort() == ["breakdown.failure", "breakdown.success"]

      assert Enum.map(result["breakdown.success"], & &1.value) == [5.0, 8.0]
      assert Enum.map(result["breakdown.failure"], & &1.value) == [3.0, 2.0]
    end
  end
end
