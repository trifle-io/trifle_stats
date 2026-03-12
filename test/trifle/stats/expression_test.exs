defmodule Trifle.Stats.ExpressionTest do
  use ExUnit.Case, async: true

  alias Trifle.Stats.Series
  alias Trifle.Stats.Transponder.Expression
  alias Trifle.Stats.Transponder.ExpressionEngine

  test "parses and evaluates arithmetic expressions" do
    {:ok, ast} = ExpressionEngine.parse("(a + b) * c", ["x", "y", "z"])
    assert {:ok, result} = ExpressionEngine.evaluate(ast, %{"a" => 1, "b" => 2, "c" => 7})
    assert to_float(result) == 21.0
  end

  test "supports built-in functions" do
    {:ok, ast} = ExpressionEngine.parse("sum(a, max(b, c))", ["a", "b", "c"])
    assert {:ok, result} = ExpressionEngine.evaluate(ast, %{"a" => 1, "b" => 2, "c" => 4})
    assert to_float(result) == 5.0
  end

  test "returns nil for divide by zero" do
    {:ok, ast} = ExpressionEngine.parse("a / b", ["sum", "count"])
    assert {:ok, nil} = ExpressionEngine.evaluate(ast, %{"a" => 10, "b" => 0})
  end

  test "applies expressions across series values" do
    series = %{
      at: [1, 2, 3],
      values: [
        %{"metrics" => %{"sum" => 30, "count" => 3}},
        %{"metrics" => %{"sum" => 20, "count" => 4}},
        %{"metrics" => %{"sum" => 0, "count" => 0}}
      ]
    }

    assert {:ok, updated} =
             Expression.transform(
               series,
               ["metrics.sum", "metrics.count"],
               "a / b",
               "metrics.average"
             )

    averages =
      updated.values
      |> Enum.map(fn %{"metrics" => metrics} -> metrics["average"] end)
      |> Enum.map(&to_float_or_nil/1)

    assert averages == [10.0, 5.0, nil]
  end

  test "creates nested response maps when missing" do
    series = %{
      at: [1],
      values: [%{"metrics" => %{"sum" => 30, "count" => 3}}]
    }

    assert {:ok, updated} =
             Expression.transform(
               series,
               ["metrics.sum", "metrics.count"],
               "a / b",
               "metrics.duration.average"
             )

    assert get_in(hd(updated.values), ["metrics", "duration", "average"]) |> to_float() == 10.0
  end

  test "returns nil when an input path is missing" do
    series = %{at: [1], values: [%{"metrics" => %{"sum" => 30}}]}

    assert {:ok, updated} =
             Expression.transform(
               series,
               ["metrics.sum", "metrics.count"],
               "a / b",
               "metrics.average"
             )

    assert get_in(hd(updated.values), ["metrics", "average"]) == nil
  end

  test "rejects wildcard paths during stage 1" do
    assert {:error, %{message: "Wildcard paths are not supported yet."}} ==
             Expression.validate(["codes.*.count"], "a", "codes.*.average")
  end

  test "series wrapper exposes transform_expression" do
    series =
      Series.new(%{
        at: [1, 2],
        values: [
          %{"metrics" => %{"sum" => 30, "count" => 3}},
          %{"metrics" => %{"sum" => 50, "count" => 10}}
        ]
      })

    updated =
      series
      |> Series.transform_expression(["metrics.sum", "metrics.count"], "a / b", "metrics.average")

    assert %Series{} = updated
    assert get_in(Enum.at(updated.series.values, 0), ["metrics", "average"]) == 10.0
    assert get_in(Enum.at(updated.series.values, 1), ["metrics", "average"]) == 5.0
  end

  test "series wrapper raises for invalid expressions" do
    series = Series.new(%{at: [1], values: [%{"metrics" => %{"sum" => 30}}]})

    assert_raise ArgumentError, "Response path is required.", fn ->
      Series.transform_expression(series, ["metrics.sum"], "a", " ")
    end
  end

  defp to_float(%Decimal{} = decimal), do: Decimal.to_float(decimal)
  defp to_float(value) when is_number(value), do: value * 1.0

  defp to_float_or_nil(nil), do: nil
  defp to_float_or_nil(value), do: to_float(value)
end
