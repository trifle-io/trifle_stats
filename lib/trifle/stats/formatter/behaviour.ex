defmodule Trifle.Stats.Formatter.Behaviour do
  @moduledoc """
  Behaviour for custom formatter implementations.
  
  Formatters transform time-series data into different presentation formats.
  They reshape data for visualization, reporting, or further processing.
  
  ## Example Implementation
  
      defmodule MyCustomFormatter do
        @behaviour Trifle.Stats.Formatter.Behaviour
        
        @impl true
        def format(series, path, slices \\ 1, transform_fn \\ nil) do
          # Extract values at the specified path
          values = series[:values] || []
          timeline = series[:at] || []
          
          # Perform custom formatting logic
          formatted_data = values
          |> Enum.with_index()
          |> Enum.map(fn {value, idx} ->
            raw_value = get_in(value, String.split(path, "."))
            transformed = if transform_fn, do: transform_fn.(raw_value), else: raw_value
            
            %{
              timestamp: Enum.at(timeline, idx),
              value: transformed,
              custom_field: "processed"
            }
          end)
          
          %{format: "custom", data: formatted_data}
        end
      end
  """
  
  @doc """
  Format data from a series at the specified path.
  
  ## Parameters
  
  - `series`: Map containing `:at` (timeline) and `:values` (data points) keys
  - `path`: String path to the values to format (e.g., "count", "metrics.duration")
  - `slices`: Number of slices for windowed formatting (optional, defaults to 1)
  - `transform_fn`: Optional transformation function to apply to each value
  
  ## Returns
  
  Formatted data structure. The exact format depends on the formatter implementation,
  but typically returns a map with structured presentation data.
  
  ## Example
  
      series = %{
        at: [~U[2025-08-18 10:00:00Z], ~U[2025-08-18 11:00:00Z]],
        values: [%{"count" => 10}, %{"count" => 15}]
      }
      
      format(series, "count", 1)
      # => %{labels: ["count"], data: [[10, 15]]}  # timeline format
      
      format(series, "count", 1, &(&1 * 2))
      # => %{labels: ["count"], data: [[20, 30]]}  # with transform
  """
  @callback format(series :: map(), path :: String.t(), slices :: integer(), transform_fn :: function() | nil) :: map()
  
  @doc """
  Format data from a series at the specified path (3-arity version).
  """
  @callback format(series :: map(), path :: String.t(), slices :: integer()) :: map()
  
  @doc """
  Format data from a series at the specified path (2-arity version).
  """
  @callback format(series :: map(), path :: String.t()) :: map()
  
  @optional_callbacks [format: 2, format: 3]
end