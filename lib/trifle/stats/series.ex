defmodule Trifle.Stats.Series do
  @moduledoc """
  Series wrapper for timeseries data with pipe-friendly processing capabilities.
  
  Provides a clean, immutable API following Elixir best practices:
  
  ## API Design Principles
  
  1. **Pipe-friendly**: Series always as first argument
  2. **Immutable**: Transformations return new Series objects
  3. **Clear naming**: Function names indicate operation type and return behavior
  4. **Single responsibility**: One clear way to perform each operation
  
  ## Usage Examples
  
  ### Terminal Operations (return computed values)
      series
      |> aggregate_sum("count")           # returns number
      |> then(&IO.inspect(&1, label: "Total Count"))
      
      timeline_data = series |> format_timeline("count")
      categories = series |> format_category("status")
      
  ### Transformation Operations (return new Series)
      new_series = series
      |> transform_average("sum", "count", "avg")
      |> transform_ratio("success", "total", "success_rate")
      
  ### Chaining Operations
      result = series
      |> transform_average("sum", "count", "avg")
      |> transform_ratio("success", "total", "rate")
      |> aggregate_max("rate")
      
      formatted_data = series
      |> transform_stddev("metrics.sum", "metrics.count", "metrics.square", "deviation")
      |> format_timeline("deviation")
  """
  
  defstruct [:series]
  
  def new(series) do
    normalized_values =
      series[:values]
      |> List.wrap()
      |> Trifle.Stats.Packer.normalize()
      |> normalize_decimals()

    normalized_series = Map.put(series, :values, normalized_values)
    
    %__MODULE__{series: normalized_series}
  end
  
  # Terminal aggregation operations (return computed values)
  def aggregate_mean(series, path, slices \\ 1) do
    Trifle.Stats.Aggregator.Mean.aggregate(series.series, path, slices)
  end
  
  def aggregate_max(series, path, slices \\ 1) do
    Trifle.Stats.Aggregator.Max.aggregate(series.series, path, slices)
  end
  
  def aggregate_min(series, path, slices \\ 1) do
    Trifle.Stats.Aggregator.Min.aggregate(series.series, path, slices)
  end
  
  def aggregate_sum(series, path, slices \\ 1) do
    Trifle.Stats.Aggregator.Sum.aggregate(series.series, path, slices)
  end
  
  # Terminal formatting operations (return formatted data)
  def format_category(series, path, slices \\ 1, transform_fn \\ nil) do
    Trifle.Stats.Formatter.Category.format(series.series, path, slices, transform_fn)
  end
  
  def format_timeline(series, path, slices \\ 1, transform_fn \\ nil) do
    Trifle.Stats.Formatter.Timeline.format(series.series, path, slices, transform_fn)
  end
  
  # Transformation operations (return new Series for chaining)
  def transform_divide(series, left_path, right_path, response_path, slices \\ 1) do
    updated_series =
      Trifle.Stats.Transponder.Divide.transform(
        series.series,
        left_path,
        right_path,
        response_path,
        slices
      )

    %__MODULE__{series: normalize_series(updated_series)}
  end

  def transform_add(series, left_path, right_path, response_path, slices \\ 1) do
    updated_series =
      Trifle.Stats.Transponder.Add.transform(
        series.series,
        left_path,
        right_path,
        response_path,
        slices
      )

    %__MODULE__{series: normalize_series(updated_series)}
  end

  def transform_multiply(series, left_path, right_path, response_path, slices \\ 1) do
    updated_series =
      Trifle.Stats.Transponder.Multiply.transform(
        series.series,
        left_path,
        right_path,
        response_path,
        slices
      )

    %__MODULE__{series: normalize_series(updated_series)}
  end

  def transform_subtract(series, left_path, right_path, response_path, slices \\ 1) do
    updated_series =
      Trifle.Stats.Transponder.Subtract.transform(
        series.series,
        left_path,
        right_path,
        response_path,
        slices
      )

    %__MODULE__{series: normalize_series(updated_series)}
  end

  def transform_sum(series, paths, response_path, slices \\ 1) do
    updated_series =
      Trifle.Stats.Transponder.Sum.transform(series.series, paths, response_path, slices)

    %__MODULE__{series: normalize_series(updated_series)}
  end

  def transform_min(series, paths, response_path, slices \\ 1) do
    updated_series =
      Trifle.Stats.Transponder.Min.transform(series.series, paths, response_path, slices)

    %__MODULE__{series: normalize_series(updated_series)}
  end

  def transform_max(series, paths, response_path, slices \\ 1) do
    updated_series =
      Trifle.Stats.Transponder.Max.transform(series.series, paths, response_path, slices)

    %__MODULE__{series: normalize_series(updated_series)}
  end

  def transform_mean(series, paths, response_path, slices \\ 1) do
    updated_series =
      Trifle.Stats.Transponder.Mean.transform(series.series, paths, response_path, slices)

    %__MODULE__{series: normalize_series(updated_series)}
  end
  
  def transform_ratio(series, sample_path, total_path, response_path, slices \\ 1) do
    updated_series =
      Trifle.Stats.Transponder.Ratio.transform(
        series.series,
        sample_path,
        total_path,
        response_path,
        slices
      )

    %__MODULE__{series: normalize_series(updated_series)}
  end
  
  def transform_stddev(series, sum_path, count_path, square_path, response_path, slices \\ 1) do
    updated_series =
      Trifle.Stats.Transponder.StandardDeviation.transform(
        series.series,
        sum_path,
        count_path,
        square_path,
        response_path,
        slices
      )

    %__MODULE__{series: normalize_series(updated_series)}
  end

  defp normalize_series(%{values: values} = series) do
    %{series | values: normalize_decimals(values)}
  end

  defp normalize_series(other), do: other

  defp normalize_decimals(%Decimal{} = decimal), do: Decimal.to_float(decimal)

  defp normalize_decimals(%DateTime{} = dt), do: dt
  defp normalize_decimals(%NaiveDateTime{} = ndt), do: ndt

  defp normalize_decimals(%{} = map) when not is_struct(map) do
    map
    |> Enum.map(fn {key, value} -> {key, normalize_decimals(value)} end)
    |> Map.new()
  end

  defp normalize_decimals(list) when is_list(list),
    do: Enum.map(list, &normalize_decimals/1)

  defp normalize_decimals(other), do: other
end
