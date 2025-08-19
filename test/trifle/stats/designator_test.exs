defmodule Trifle.Stats.DesignatorTest do
  use ExUnit.Case

  describe "designator functions" do
    test "linear designator works" do
      # Test basic linear classification
      linear = Trifle.Stats.Designator.Linear.new(0, 100, 10)
      result = Trifle.Stats.Designator.Linear.designate(linear, 50)
      assert result == "50"
      
      # Test edge cases
      result_min = Trifle.Stats.Designator.Linear.designate(linear, 0)
      assert result_min == "0"
      
      result_max = Trifle.Stats.Designator.Linear.designate(linear, 100)
      assert result_max == "100"
    end
    
    test "geometric designator works" do
      # Test basic geometric classification  
      geometric = Trifle.Stats.Designator.Geometric.new(1, 2)
      result = Trifle.Stats.Designator.Geometric.designate(geometric, 16)
      assert result == "2.0+"
      
      # Test with different ratio
      geometric2 = Trifle.Stats.Designator.Geometric.new(1, 200)
      result2 = Trifle.Stats.Designator.Geometric.designate(geometric2, 125)
      assert result2 == "1.0e3"
    end
    
    test "custom designator works" do
      # Test with custom granularities
      granularities = [0, 10, 25, 50, 100]
      custom = Trifle.Stats.Designator.Custom.new(granularities)
      result = Trifle.Stats.Designator.Custom.designate(custom, 30)
      assert result == "50"
      
      # Test edge cases
      result_first = Trifle.Stats.Designator.Custom.designate(custom, 5)
      assert result_first == "10"
      
      result_last = Trifle.Stats.Designator.Custom.designate(custom, 75)
      assert result_last == "100"
    end
  end
end