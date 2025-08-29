defmodule Trifle.Stats.Series.Fluent do
  @moduledoc """
  Enhanced fluent interface for Series operations with proper method chaining.
  
  This module provides a more idiomatic Elixir fluent interface that works
  seamlessly with the pipe operator and supports both terminal and intermediate
  operations for proper method chaining.
  
  ## Usage Patterns
  
  ### Basic fluent chaining
      series 
      |> aggregate_avg("count")      # intermediate - returns Series  
      |> format_timeline("count")    # terminal - returns formatted data
      
  ### Pipe operator integration
      series
      |> pipe_aggregate()
      |> avg("count")
      |> pipe_format() 
      |> timeline("count")
      
  ### Terminal vs Intermediate operations
      # Terminal operations return raw data (final results)
      result = series |> aggregate_avg("count")  # returns number
      
      # Intermediate operations return Series (for further chaining)
      new_series = series |> transpond_average("sum,count", "avg")  # returns Series
      
  ### Mixed operation chaining
      series
      |> transpond_average("sum,count", "avg")    # Series -> Series
      |> aggregate_max("avg")                     # Series -> raw data
      |> then(&IO.inspect(&1, label: "Max avg"))  # continue with raw data
  """
  
  @doc """
  Import this module to get fluent methods on Series structs.
  """
  defmacro __using__(_opts) do
    quote do
      import Trifle.Stats.Series.Fluent
    end
  end
  
  # =============================================================================
  # AGGREGATOR FLUENT METHODS (Terminal Operations - Return Raw Data)
  # =============================================================================
  
  @doc """
  Fluent average aggregation. Returns the average value (terminal operation).
  
  ## Examples
      series |> aggregate_avg("count")
      # => 15.5
      
      series |> aggregate_avg("count", 2)  # with slices
      # => [10.0, 21.0]
  """
  def aggregate_mean(%Trifle.Stats.Series{} = series, path, slices \\ 1) do
    Trifle.Stats.Aggregator.Mean.aggregate(series.series, path, slices)
  end
  
  @doc """
  Fluent sum aggregation. Returns the sum value (terminal operation).
  """  
  def aggregate_sum(%Trifle.Stats.Series{} = series, path, slices \\ 1) do
    Trifle.Stats.Aggregator.Sum.aggregate(series.series, path, slices)
  end
  
  @doc """
  Fluent max aggregation. Returns the maximum value (terminal operation).
  """
  def aggregate_max(%Trifle.Stats.Series{} = series, path, slices \\ 1) do
    Trifle.Stats.Aggregator.Max.aggregate(series.series, path, slices)
  end
  
  @doc """
  Fluent min aggregation. Returns the minimum value (terminal operation).
  """
  def aggregate_min(%Trifle.Stats.Series{} = series, path, slices \\ 1) do
    Trifle.Stats.Aggregator.Min.aggregate(series.series, path, slices)
  end
  
  @doc """
  Call a custom registered aggregator (terminal operation).
  """
  def aggregate_call(%Trifle.Stats.Series{} = series, method_name, args \\ []) when is_atom(method_name) do
    aggregator = Trifle.Stats.Series.Aggregator.new(series)
    Trifle.Stats.Series.Registry.call_aggregator(aggregator, method_name, args)
  end
  
  # =============================================================================
  # FORMATTER FLUENT METHODS (Terminal Operations - Return Raw Data) 
  # =============================================================================
  
  @doc """
  Fluent timeline formatting. Returns formatted timeline data (terminal operation).
  
  ## Examples
      series |> format_timeline("count")
      # => %{labels: [...], data: [...]}
  """
  def format_timeline(%Trifle.Stats.Series{} = series, path, slices \\ 1, transform_fn \\ nil) do
    Trifle.Stats.Formatter.Timeline.format(series.series, path, slices, transform_fn)
  end
  
  @doc """
  Fluent category formatting. Returns formatted category data (terminal operation).
  """
  def format_category(%Trifle.Stats.Series{} = series, path, slices \\ 1, transform_fn \\ nil) do
    Trifle.Stats.Formatter.Category.format(series.series, path, slices, transform_fn)
  end
  
  @doc """
  Call a custom registered formatter (terminal operation).
  """
  def format_call(%Trifle.Stats.Series{} = series, method_name, args \\ []) when is_atom(method_name) do
    formatter = Trifle.Stats.Series.Formatter.new(series)
    Trifle.Stats.Series.Registry.call_formatter(formatter, method_name, args)
  end
  
  # =============================================================================
  # TRANSPONDER FLUENT METHODS (Intermediate Operations - Return Series)
  # =============================================================================
  
  @doc """
  Fluent add transponder. Returns transformed Series (intermediate operation).
  
  ## Examples
      series 
      |> transpond_add("value_a", "value_b", "total")
      |> aggregate_max("total")  # can chain further operations
  """
  def transpond_add(%Trifle.Stats.Series{} = series, left_path, right_path, response_path, slices \\ 1) do
    updated_series = Trifle.Stats.Transponder.Add.transform(series.series, left_path, right_path, response_path, slices)
    %Trifle.Stats.Series{series: updated_series}
  end

  def transpond_divide(%Trifle.Stats.Series{} = series, left_path, right_path, response_path, slices \\ 1) do
    updated_series = Trifle.Stats.Transponder.Divide.transform(series.series, left_path, right_path, response_path, slices)
    %Trifle.Stats.Series{series: updated_series}
  end

  def transpond_multiply(%Trifle.Stats.Series{} = series, left_path, right_path, response_path, slices \\ 1) do
    updated_series = Trifle.Stats.Transponder.Multiply.transform(series.series, left_path, right_path, response_path, slices)
    %Trifle.Stats.Series{series: updated_series}
  end

  def transpond_subtract(%Trifle.Stats.Series{} = series, left_path, right_path, response_path, slices \\ 1) do
    updated_series = Trifle.Stats.Transponder.Subtract.transform(series.series, left_path, right_path, response_path, slices)
    %Trifle.Stats.Series{series: updated_series}
  end

  def transpond_sum(%Trifle.Stats.Series{} = series, values_path, response_path, slices \\ 1) do
    updated_series = Trifle.Stats.Transponder.Sum.transform(series.series, values_path, response_path, slices)
    %Trifle.Stats.Series{series: updated_series}
  end

  def transpond_min(%Trifle.Stats.Series{} = series, values_path, response_path, slices \\ 1) do
    updated_series = Trifle.Stats.Transponder.Min.transform(series.series, values_path, response_path, slices)
    %Trifle.Stats.Series{series: updated_series}
  end

  def transpond_max(%Trifle.Stats.Series{} = series, values_path, response_path, slices \\ 1) do
    updated_series = Trifle.Stats.Transponder.Max.transform(series.series, values_path, response_path, slices)
    %Trifle.Stats.Series{series: updated_series}
  end

  def transpond_mean(%Trifle.Stats.Series{} = series, values_path, response_path, slices \\ 1) do
    updated_series = Trifle.Stats.Transponder.Mean.transform(series.series, values_path, response_path, slices)
    %Trifle.Stats.Series{series: updated_series}
  end
  
  @doc """
  Fluent ratio transponder. Returns transformed Series (intermediate operation).
  """
  def transpond_ratio(%Trifle.Stats.Series{} = series, sample_path, total_path, response_path, slices \\ 1) do
    updated_series = Trifle.Stats.Transponder.Ratio.transform(series.series, sample_path, total_path, response_path, slices)
    %Trifle.Stats.Series{series: updated_series}
  end
  
  @doc """
  Fluent standard deviation transponder. Returns transformed Series (intermediate operation).
  """
  def transpond_standard_deviation(%Trifle.Stats.Series{} = series, source_path, target_path, slices \\ 1, sample_stddev \\ false) do
    updated_series = Trifle.Stats.Transponder.StandardDeviation.transform(series.series, source_path, target_path, slices, sample_stddev)
    %Trifle.Stats.Series{series: updated_series}
  end
  
  @doc """
  Call a custom registered transponder (intermediate operation).
  """
  def transpond_call(%Trifle.Stats.Series{} = series, method_name, args \\ []) when is_atom(method_name) do
    transponder = Trifle.Stats.Series.Transponder.new(series)
    Trifle.Stats.Series.Registry.call_transponder(transponder, method_name, args)
  end
  
  # =============================================================================
  # PIPE OPERATOR INTEGRATION
  # =============================================================================
  
  @doc """
  Enter aggregation context for pipe operator chaining.
  
  ## Examples  
      series
      |> pipe_aggregate()
      |> avg("count")
      |> then(&IO.inspect(&1, label: "Average"))
  """
  def pipe_aggregate(%Trifle.Stats.Series{} = series) do
    Trifle.Stats.Series.Aggregator.new(series)
  end
  
  @doc """
  Enter formatting context for pipe operator chaining.
  
  ## Examples
      series
      |> pipe_format()
      |> timeline("count")
      |> then(&IO.inspect(&1, label: "Timeline"))
  """
  def pipe_format(%Trifle.Stats.Series{} = series) do
    Trifle.Stats.Series.Formatter.new(series)
  end
  
  @doc """
  Enter transponder context for pipe operator chaining.
  
  ## Examples
      series
      |> pipe_transpond()
      |> average("sum,count", "avg")  # returns Series for further chaining
  """
  def pipe_transpond(%Trifle.Stats.Series{} = series) do
    Trifle.Stats.Series.Transponder.new(series)
  end
  
  # =============================================================================
  # CONVENIENCE METHODS 
  # =============================================================================
  
  @doc """
  Chain multiple transponder operations.
  
  ## Examples
      series
      |> chain_transpond([
        {:average, ["sum,count", "avg"]},
        {:ratio, ["errors,total", "error_rate"]}
      ])
      |> aggregate_max("error_rate")
  """
  def chain_transpond(%Trifle.Stats.Series{} = series, operations) when is_list(operations) do
    Enum.reduce(operations, series, fn {operation, args}, acc_series ->
      apply(__MODULE__, :"transpond_#{operation}", [acc_series | args])
    end)
  end
  
  @doc """
  Apply multiple aggregations and return results as a map.
  
  ## Examples
      series |> aggregate_multiple([
        avg: ["count"],
        max: ["duration"],
        sum: ["count", 2]  # with slices
      ])
      # => %{avg: 15.5, max: 200, sum: [10, 20]}
  """  
  def aggregate_multiple(%Trifle.Stats.Series{} = series, operations) when is_list(operations) do
    Map.new(operations, fn {operation, args} ->
      result = apply(__MODULE__, :"aggregate_#{operation}", [series | args])
      {operation, result}
    end)
  end
  
  @doc """
  Apply multiple formatters and return results as a map.
  """
  def format_multiple(%Trifle.Stats.Series{} = series, operations) when is_list(operations) do
    Map.new(operations, fn {operation, args} ->
      result = apply(__MODULE__, :"format_#{operation}", [series | args])
      {operation, result}
    end)
  end
  
  @doc """
  Debug helper - inspect series data and pass through for chaining.
  
  ## Examples
      series
      |> debug_inspect("Before transformation")
      |> transpond_average("sum,count", "avg")
      |> debug_inspect("After transformation")
      |> aggregate_max("avg")
  """
  def debug_inspect(%Trifle.Stats.Series{} = series, label \\ "Series") do
    IO.inspect(series.series, label: label)
    series
  end
end