defmodule Trifle.Stats.DynamicMethodsTest do
  use ExUnit.Case
  
  # Test sample data
  @sample_series %{
    at: [~U[2025-08-18 10:00:00Z], ~U[2025-08-18 11:00:00Z]],
    values: [%{"count" => 10}, %{"count" => 20}]
  }
  
  @transponder_series %{
    at: [~U[2025-08-18 10:00:00Z], ~U[2025-08-18 11:00:00Z]],
    values: [%{"sum" => 100, "count" => 10}, %{"sum" => 200, "count" => 20}]
  }
  
  # Test custom implementations
  defmodule TestWeightedAvgAggregator do
    @behaviour Trifle.Stats.Aggregator.Behaviour
    
    @impl true
    def aggregate(series, path, _slices \\ 1) do
      values = series[:values] || []
      
      # Extract value and weight paths
      [value_path, weight_path] = String.split(path, ",")
      
      {total_value, total_weight} = 
        Enum.reduce(values, {0, 0}, fn item, {val_acc, weight_acc} ->
          val = get_in(item, String.split(value_path, ".")) || 0
          weight = get_in(item, String.split(weight_path, ".")) || 0
          {val_acc + (val * weight), weight_acc + weight}
        end)
      
      if total_weight > 0, do: total_value / total_weight, else: 0
    end
  end
  
  defmodule TestJsonFormatter do
    @behaviour Trifle.Stats.Formatter.Behaviour
    
    @impl true
    def format(series, path, _slices \\ 1, _transform_fn \\ nil) do
      values = series[:values] || []
      timeline = series[:at] || []
      
      data = values
      |> Enum.with_index()
      |> Enum.map(fn {value, idx} ->
        %{
          timestamp: Enum.at(timeline, idx),
          value: get_in(value, String.split(path, ".")),
          format: "json_export"
        }
      end)
      
      %{format: "json", data: data, count: length(data)}
    end
  end
  
  defmodule TestNormalizeTransponder do
    @behaviour Trifle.Stats.Transponder.Behaviour
    
    @impl true
    def transform(series, source_path, target_path, _slices \\ 1, _options \\ []) do
      values = series[:values] || []
      
      # Find min and max for normalization
      extracted_values = Enum.map(values, &get_in(&1, String.split(source_path, ".")))
      |> Enum.reject(&is_nil/1)
      
      min_val = Enum.min(extracted_values)
      max_val = Enum.max(extracted_values)
      granularity = max_val - min_val
      
      # Normalize values to 0-1 granularity
      transformed_values = Enum.map(values, fn value ->
        source_value = get_in(value, String.split(source_path, "."))
        
        normalized_value = if source_value && granularity > 0 do
          (source_value - min_val) / granularity
        else
          0.0
        end
        
        put_in(value, String.split(target_path, "."), normalized_value)
      end)
      
      Map.put(series, :values, transformed_values)
    end
  end
  
  # Test module using dynamic registration
  defmodule TestAnalyticsModule do
    use Trifle.Stats.Series.Dynamic
    
    # Register custom components at compile time
    register_aggregator :weighted_avg, TestWeightedAvgAggregator
    register_formatter :json_export, TestJsonFormatter
    register_transponder :normalize, TestNormalizeTransponder
    
    def analyze_series(series) do
      series
      |> transpond_normalize("count", "normalized")
      |> aggregate_weighted_avg("normalized,count")
    end
    
    def export_data(series) do
      series |> format_json_export("count")
    end
  end
  
  describe "compile-time method generation" do
    test "generates aggregator methods" do
      series = Trifle.Stats.Series.new(@transponder_series)
      
      # Should have generated aggregate_weighted_avg/3
      assert function_exported?(TestAnalyticsModule, :aggregate_weighted_avg, 3)
      assert function_exported?(TestAnalyticsModule, :aggregate_weighted_avg, 4)
      
      # Method should work
      result = TestAnalyticsModule.aggregate_weighted_avg(series, "sum,count")
      assert is_number(result)
      assert result > 0
    end
    
    test "generates formatter methods" do
      series = Trifle.Stats.Series.new(@sample_series)
      
      # Should have generated format_json_export/4
      assert function_exported?(TestAnalyticsModule, :format_json_export, 4)
      assert function_exported?(TestAnalyticsModule, :format_json_export, 5)
      
      # Method should work
      result = TestAnalyticsModule.format_json_export(series, "count")
      assert result.format == "json"
      assert Map.has_key?(result, :data)
      assert length(result.data) == 2
    end
    
    test "generates transponder methods" do
      series = Trifle.Stats.Series.new(@sample_series)
      
      # Should have generated transpond_normalize/4
      assert function_exported?(TestAnalyticsModule, :transpond_normalize, 4)
      assert function_exported?(TestAnalyticsModule, :transpond_normalize, 5)
      
      # Method should work and return Series
      result = TestAnalyticsModule.transpond_normalize(series, "count", "normalized")
      assert %Trifle.Stats.Series{} = result
      
      # Check normalization worked
      values = result.series[:values]
      first_normalized = Enum.at(values, 0)["normalized"]
      second_normalized = Enum.at(values, 1)["normalized"]
      
      assert first_normalized == 0.0    # min value normalized to 0
      assert second_normalized == 1.0   # max value normalized to 1
    end
    
    test "generated methods support method chaining" do
      series = Trifle.Stats.Series.new(@sample_series)
      
      # Can chain transponder -> aggregator
      result = series
      |> TestAnalyticsModule.transpond_normalize("count", "normalized")
      |> TestAnalyticsModule.aggregate_weighted_avg("normalized,count")
      
      assert is_number(result)
    end
    
    test "module provides metadata about registered components" do
      components = TestAnalyticsModule.__get_registered_components__()
      
      assert Map.has_key?(components, :aggregators)
      assert Map.has_key?(components, :formatters)
      assert Map.has_key?(components, :transponders)
      
      assert {:weighted_avg, TestWeightedAvgAggregator} in components.aggregators
      assert {:json_export, TestJsonFormatter} in components.formatters
      assert {:normalize, TestNormalizeTransponder} in components.transponders
    end
  end
  
  describe "runtime registration" do
    test "components are registered in the registry" do
      # Ensure components are registered
      TestAnalyticsModule.__register_components__()
      
      # Check they're in the registry
      {:ok, TestWeightedAvgAggregator} = Trifle.Stats.Series.Registry.get_aggregator(:weighted_avg)
      {:ok, TestJsonFormatter} = Trifle.Stats.Series.Registry.get_formatter(:json_export)
      {:ok, TestNormalizeTransponder} = Trifle.Stats.Series.Registry.get_transponder(:normalize)
    end
    
    @tag :skip
    test "registered components work via registry call methods (deprecated API)" do
      # This test demonstrates the old registry system which needs redesign
      # for the new pipe-friendly API structure
      TestAnalyticsModule.__register_components__()
      
      series = Trifle.Stats.Series.new(@sample_series)
      
      # Built-in aggregators work with new API
      result = Trifle.Stats.Series.aggregate_sum(series, "count")
      assert is_number(result)
    end
  end
  
  describe "integration with fluent interface" do
    test "generated methods work with fluent interface imports" do
      # Import fluent methods
      import Trifle.Stats.Series.Fluent
      
      series = Trifle.Stats.Series.new(@sample_series)
      
      # Mix generated methods with fluent methods
      result = series
      |> TestAnalyticsModule.transpond_normalize("count", "normalized")
      |> aggregate_sum("normalized")  # use fluent method

      assert is_number(hd(result))
      assert hd(result) >= 0
    end
    
    test "can chain between generated and fluent methods" do
      import Trifle.Stats.Series.Fluent
      
      series = Trifle.Stats.Series.new(@transponder_series)
      
      result = series
      |> transpond_divide("sum", "count", "avg")        # fluent transponder
      |> TestAnalyticsModule.transpond_normalize("avg", "norm_avg")  # generated transponder
      |> aggregate_max("norm_avg")                     # fluent aggregator

      assert is_number(hd(result))
    end
  end
  
  describe "advanced usage patterns" do
    test "can build complex analysis pipelines" do
      import Trifle.Stats.Series.Fluent
      
      series = Trifle.Stats.Series.new(@transponder_series)
      
      # Complex pipeline using multiple generated and fluent methods
      pipeline_result = series
      |> transpond_divide("sum", "count", "avg")                     # fluent
      |> TestAnalyticsModule.transpond_normalize("avg", "norm")    # generated
      |> debug_inspect("After normalization")                     # fluent debug
      |> aggregate_multiple([                                     # fluent multi-agg
        max: ["norm"],
        min: ["norm"], 
        mean: ["norm"]
      ])
      
      assert Map.has_key?(pipeline_result, :max)
      assert Map.has_key?(pipeline_result, :min)
      assert Map.has_key?(pipeline_result, :mean)
    end
    
    test "custom method definitions work as expected" do
      series = Trifle.Stats.Series.new(@transponder_series)
      
      # Use the custom analyze method
      result = TestAnalyticsModule.analyze_series(series)
      assert is_number(result)
      
      # Use the custom export method
      export_result = TestAnalyticsModule.export_data(series)
      assert export_result.format == "json"
      assert is_list(export_result.data)
    end
    
    test "supports method overloading with additional arguments" do
      series = Trifle.Stats.Series.new(@sample_series)
      
      # Basic method call
      result1 = TestAnalyticsModule.transpond_normalize(series, "count", "norm")
      
      # Method call with additional options (tests the 5-arity version)
      result2 = TestAnalyticsModule.transpond_normalize(series, "count", "norm", 1, scale: 10)
      
      assert %Trifle.Stats.Series{} = result1
      assert %Trifle.Stats.Series{} = result2
    end
  end
  
  describe "error handling" do
    test "generated methods validate input types" do
      # Should work with Series struct
      series = Trifle.Stats.Series.new(@sample_series)
      result = TestAnalyticsModule.aggregate_weighted_avg(series, "count,count")
      assert is_number(result)
      
      # Should fail with wrong type
      assert_raise FunctionClauseError, fn ->
        TestAnalyticsModule.aggregate_weighted_avg(%{invalid: "data"}, "count,count")
      end
    end
    
    test "handles missing paths gracefully in generated methods" do
      series = Trifle.Stats.Series.new(@sample_series)
      
      # Should handle missing paths without crashing
      result = TestAnalyticsModule.aggregate_weighted_avg(series, "missing,count")
      assert result == 0  # weighted avg should return 0 for missing data
    end
  end
end
