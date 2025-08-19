defmodule Trifle.Stats.FormatterTest do
  use ExUnit.Case

  describe "formatter functions" do
    test "category formatter works" do
      data = %{
        at: [~U[2025-08-17 10:00:00Z], ~U[2025-08-17 11:00:00Z]],
        values: [
          %{"0" => 5, "1" => 10, "2" => 3},
          %{"0" => 8, "1" => 12, "2" => 1}
        ]
      }
      
      result = Trifle.Stats.Formatter.Category.format(data, "0")
      
      assert is_list(result)
      assert length(result) == 2
      
      [first, second] = result
      assert %{at: ~U[2025-08-17 10:00:00Z], value: 5.0} = first
      assert %{at: ~U[2025-08-17 11:00:00Z], value: 8.0} = second
    end
    
    test "timeline formatter works" do
      data = %{
        at: [~U[2025-08-17 10:00:00Z], ~U[2025-08-17 11:00:00Z]],
        values: [%{count: 5}, %{count: 10}]
      }
      
      result = Trifle.Stats.Formatter.Timeline.format(data, "count")
      
      assert is_list(result)
      assert length(result) == 2
      
      [first, second] = result
      assert %{at: ~U[2025-08-17 10:00:00Z], value: 5.0} = first
      assert %{at: ~U[2025-08-17 11:00:00Z], value: 10.0} = second
    end
  end
end