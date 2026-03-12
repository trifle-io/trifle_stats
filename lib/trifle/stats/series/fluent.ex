defmodule Trifle.Stats.Series.Fluent do
  @moduledoc """
  Fluent helpers for `%Trifle.Stats.Series{}`.

  Aggregations and formatters are terminal operations. The built-in transponder
  surface is expression-only:

      series
      |> transpond_expression(["sum", "count"], "a / b", "avg")
      |> aggregate_max("avg")
  """

  @doc """
  Import this module to get fluent methods on Series structs.
  """
  defmacro __using__(_opts) do
    quote do
      import Trifle.Stats.Series.Fluent
    end
  end

  def aggregate_mean(%Trifle.Stats.Series{} = series, path, slices \\ 1) do
    Trifle.Stats.Aggregator.Mean.aggregate(series.series, path, slices)
  end

  def aggregate_sum(%Trifle.Stats.Series{} = series, path, slices \\ 1) do
    Trifle.Stats.Aggregator.Sum.aggregate(series.series, path, slices)
  end

  def aggregate_max(%Trifle.Stats.Series{} = series, path, slices \\ 1) do
    Trifle.Stats.Aggregator.Max.aggregate(series.series, path, slices)
  end

  def aggregate_min(%Trifle.Stats.Series{} = series, path, slices \\ 1) do
    Trifle.Stats.Aggregator.Min.aggregate(series.series, path, slices)
  end

  def aggregate_call(%Trifle.Stats.Series{} = series, method_name, args \\ [])
      when is_atom(method_name) do
    Trifle.Stats.Series.Registry.call_aggregator(series, method_name, args)
  end

  def format_timeline(%Trifle.Stats.Series{} = series, path, slices \\ 1, transform_fn \\ nil) do
    Trifle.Stats.Formatter.Timeline.format(series.series, path, slices, transform_fn)
  end

  def format_category(%Trifle.Stats.Series{} = series, path, slices \\ 1, transform_fn \\ nil) do
    Trifle.Stats.Formatter.Category.format(series.series, path, slices, transform_fn)
  end

  def format_call(%Trifle.Stats.Series{} = series, method_name, args \\ [])
      when is_atom(method_name) do
    Trifle.Stats.Series.Registry.call_formatter(series, method_name, args)
  end

  def transpond_expression(
        %Trifle.Stats.Series{} = series,
        paths,
        expression,
        response,
        slices \\ 1
      ) do
    updated_series =
      case Trifle.Stats.Transponder.Expression.transform(
             series.series,
             paths,
             expression,
             response,
             slices
           ) do
        {:ok, result} -> result
        {:error, %{message: message}} -> raise ArgumentError, message
        {:error, error} -> raise ArgumentError, inspect(error)
      end

    %Trifle.Stats.Series{series: updated_series}
  end

  def transpond_call(%Trifle.Stats.Series{} = series, method_name, args \\ [])
      when is_atom(method_name) do
    Trifle.Stats.Series.Registry.call_transponder(series, method_name, args)
  end

  def pipe_aggregate(%Trifle.Stats.Series{} = series) do
    series
  end

  def pipe_format(%Trifle.Stats.Series{} = series) do
    series
  end

  def pipe_transpond(%Trifle.Stats.Series{} = series) do
    series
  end

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
      |> transpond_expression(["sum", "count"], "a / b", "avg")
      |> debug_inspect("After transformation")
      |> aggregate_max("avg")
  """
  def debug_inspect(%Trifle.Stats.Series{} = series, label \\ "Series") do
    IO.inspect(series.series, label: label)
    series
  end
end
