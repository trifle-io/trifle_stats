defmodule Trifle.Stats.Series do
  @moduledoc """
  Series wrapper for timeseries data with pipe-friendly processing capabilities.

  Terminal operations return computed values:

      series
      |> aggregate_sum("count")
      |> then(&IO.inspect(&1, label: "Total Count"))

      timeline_data = series |> format_timeline("count")
      categories = series |> format_category("status")

  Transformation operations return a new `%Trifle.Stats.Series{}`:

      new_series = series
      |> transform_expression(["sum", "count"], "a / b", "avg")
      |> transform_expression(["success", "total"], "(a / b) * 100", "success_rate")

      result = series
      |> transform_expression(["sum", "count"], "a / b", "avg")
      |> transform_expression(["success", "total"], "(a / b) * 100", "rate")
      |> aggregate_max("rate")
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

  def format_category(series, path, slices \\ 1, transform_fn \\ nil) do
    Trifle.Stats.Formatter.Category.format(series.series, path, slices, transform_fn)
  end

  def format_timeline(series, path, slices \\ 1, transform_fn \\ nil) do
    Trifle.Stats.Formatter.Timeline.format(series.series, path, slices, transform_fn)
  end

  def transform_expression(series, paths, expression, response, slices \\ 1) do
    case Trifle.Stats.Transponder.Expression.transform(
           series.series,
           paths,
           expression,
           response,
           slices
         ) do
      {:ok, updated_series} ->
        %__MODULE__{series: normalize_series(updated_series)}

      {:error, %{message: message}} ->
        raise ArgumentError, message

      {:error, error} ->
        raise ArgumentError, inspect(error)
    end
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
