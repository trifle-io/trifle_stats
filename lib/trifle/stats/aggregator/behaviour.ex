defmodule Trifle.Stats.Aggregator.Behaviour do
  @moduledoc """
  Behaviour for custom aggregator implementations.
  
  Aggregators process time-series data to compute aggregate values across data points.
  They take a series with timeline and values, and return aggregated results.
  
  ## Example Implementation
  
      defmodule MyCustomAggregator do
        @behaviour Trifle.Stats.Aggregator.Behaviour
        
        @impl true
        def aggregate(series, path, slices \\ 1) do
          # Extract values at the specified path
          values = series[:values] || []
          
          # Perform custom aggregation logic
          result = values
          |> Enum.map(&get_in(&1, String.split(path, ".")))
          |> Enum.reject(&is_nil/1)
          |> custom_aggregation_logic()
          
          # Return result in expected format
          [result]
        end
        
        defp custom_aggregation_logic(values) do
          # Your custom logic here
          Enum.sum(values) / length(values)
        end
      end
  """
  
  @doc """
  Aggregate data from a series at the specified path.
  
  ## Parameters
  
  - `series`: Map containing `:at` (timeline) and `:values` (data points) keys
  - `path`: String path to the values to aggregate (e.g., "count", "metrics.duration")
  - `slices`: Number of slices for windowed aggregation (optional, defaults to 1)
  
  ## Returns
  
  List of aggregated values. For single aggregation, returns single-item list.
  For sliced aggregation, returns list with one item per slice.
  
  ## Example
  
      series = %{
        at: [~U[2025-08-18 10:00:00Z], ~U[2025-08-18 11:00:00Z]],
        values: [%{"count" => 10}, %{"count" => 15}]
      }
      
      aggregate(series, "count", 1)
      # => [25]  # sum of all values
      
      aggregate(series, "count", 2) 
      # => [10, 15]  # each slice separately
  """
  @callback aggregate(series :: map(), path :: String.t(), slices :: integer()) :: list()
  
  @doc """
  Aggregate data from a series at the specified path (2-arity version).
  
  Same as aggregate/3 but with default slices=1.
  """
  @callback aggregate(series :: map(), path :: String.t()) :: list()
  
  @optional_callbacks aggregate: 2
end