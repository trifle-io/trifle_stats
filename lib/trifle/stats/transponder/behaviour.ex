defmodule Trifle.Stats.Transponder.Behaviour do
  @moduledoc """
  Behaviour for custom transponder implementations.
  
  Transponders perform mathematical transformations on existing data, typically
  creating new derived values from existing ones. They use explicit argument paths
  instead of comma-separated strings for clarity and type safety.
  
  ## Built-in Transponders
  
  The library provides three built-in transponders with different signatures:
  
  ### Average Transponder
      Trifle.Stats.Transponder.Average.transform(series, sum_path, count_path, response_path, slices)
  
  ### Ratio Transponder  
      Trifle.Stats.Transponder.Ratio.transform(series, sample_path, total_path, response_path, slices)
  
  ### StandardDeviation Transponder
      Trifle.Stats.Transponder.StandardDeviation.transform(series, sum_path, count_path, square_path, response_path, slices)
  
  ## Custom Implementation Example
  
      defmodule MyCustomTransponder do
        @behaviour Trifle.Stats.Transponder.Behaviour
        
        @impl true
        def transform(series, source_path, target_path, slices \\ 1) do
          # Extract values from the series
          values = series[:values] || []
          
          # Transform each value
          transformed_values = Enum.map(values, fn value ->
            source_value = get_in(value, String.split(source_path, "."))
            
            # Perform custom transformation
            transformed_value = if source_value do
              source_value * 1.5  # Example: multiply by 1.5
            else
              nil
            end
            
            # Set the transformed value at target path
            put_in(value, String.split(target_path, "."), transformed_value)
          end)
          
          # Return updated series
          Map.put(series, :values, transformed_values)
        end
      end
  """
  
  
  # Since different transponders have different signatures, we define the
  # behavior more generically. Each transponder implements a transform function
  # with the appropriate arity for their specific use case.
  
  @doc """
  Transform function for basic transponders (4 parameters).
  
  Used by custom transponders with simple source->target transformations.
  """
  @callback transform(series :: map(), path1 :: String.t(), path2 :: String.t(), slices :: integer()) :: map()
  
  @doc """
  Transform function for dual-input transponders (5 parameters).
  
  Used by Average and Ratio transponders:
  - Average: transform(series, sum_path, count_path, response_path, slices)
  - Ratio: transform(series, sample_path, total_path, response_path, slices)
  """
  @callback transform(series :: map(), path1 :: String.t(), path2 :: String.t(), response_path :: String.t(), slices :: integer()) :: map()
  
  @doc """
  Transform function for triple-input transponders (6 parameters).
  
  Used by StandardDeviation transponder:
  - StandardDeviation: transform(series, sum_path, count_path, square_path, response_path, slices)
  """
  @callback transform(series :: map(), path1 :: String.t(), path2 :: String.t(), path3 :: String.t(), response_path :: String.t(), slices :: integer()) :: map()
  
  # All callbacks are optional since different transponders implement different arities
  @optional_callbacks [transform: 4, transform: 5, transform: 6]
end