defmodule Trifle.Stats.Series.Dynamic do
  @moduledoc """
  Dynamic method generation for Series operations using macros.
  
  This module provides macros to generate fluent methods for custom
  registered aggregators, formatters, and transponders, achieving similar
  functionality to Ruby's `define_method` approach.
  
  ## Usage
  
      # In your module that uses custom components
      defmodule MyAnalytics do
        use Trifle.Stats.Series.Dynamic
        
        # Register custom components at compile time
        register_aggregator :weighted_avg, MyWeightedAvgAggregator
        register_formatter :json_export, MyJsonFormatter
        register_transponder :normalize, MyNormalizer
        
        def analyze(series) do
          series
          |> aggregate_weighted_avg("value", "weight")  # auto-generated
          |> transpond_normalize("data", "normalized")  # auto-generated  
          |> format_json_export("normalized")           # auto-generated
        end
      end
  """
  
  @doc """
  Use this module to enable dynamic method generation.
  """
  defmacro __using__(_opts) do
    quote do
      import Trifle.Stats.Series.Dynamic
      import Trifle.Stats.Series.Fluent
      
      # Module attribute to track registered components at compile time
      Module.register_attribute(__MODULE__, :registered_aggregators, accumulate: true)
      Module.register_attribute(__MODULE__, :registered_formatters, accumulate: true)
      Module.register_attribute(__MODULE__, :registered_transponders, accumulate: true)
      
      @before_compile Trifle.Stats.Series.Dynamic
    end
  end
  
  @doc """
  Register a custom aggregator and generate a fluent method for it.
  
  ## Examples
      register_aggregator :weighted_avg, MyWeightedAvgAggregator
      
      # Generates method:
      # def aggregate_weighted_avg(series, path, opts \\ [])
  """
  defmacro register_aggregator(name, module) do
    quote do
      @registered_aggregators {unquote(name), unquote(module)}
      
      # Also register at runtime
      case Process.whereis(Trifle.Stats.Series.Registry) do
        pid when is_pid(pid) ->
          Trifle.Stats.Series.Registry.register_aggregator(unquote(name), unquote(module))
        nil ->
          # Registry not started yet, will be registered in __before_compile__
          :ok
      end
    end
  end
  
  @doc """
  Register a custom formatter and generate a fluent method for it.
  """
  defmacro register_formatter(name, module) do
    quote do
      @registered_formatters {unquote(name), unquote(module)}
      
      # Also register at runtime  
      case Process.whereis(Trifle.Stats.Series.Registry) do
        pid when is_pid(pid) ->
          Trifle.Stats.Series.Registry.register_formatter(unquote(name), unquote(module))
        nil ->
          :ok
      end
    end
  end
  
  @doc """
  Register a custom transponder and generate a fluent method for it.
  """
  defmacro register_transponder(name, module) do
    quote do
      @registered_transponders {unquote(name), unquote(module)}
      
      # Also register at runtime
      case Process.whereis(Trifle.Stats.Series.Registry) do
        pid when is_pid(pid) ->
          Trifle.Stats.Series.Registry.register_transponder(unquote(name), unquote(module))
        nil ->
          :ok
      end
    end
  end
  
  @doc """
  Before compile hook - generates methods for all registered components.
  """
  defmacro __before_compile__(env) do
    aggregators = Module.get_attribute(env.module, :registered_aggregators, [])
    formatters = Module.get_attribute(env.module, :registered_formatters, [])
    transponders = Module.get_attribute(env.module, :registered_transponders, [])
    
    aggregator_methods = Enum.map(aggregators, &generate_aggregator_method/1)
    formatter_methods = Enum.map(formatters, &generate_formatter_method/1)  
    transponder_methods = Enum.map(transponders, &generate_transponder_method/1)
    runtime_registration = generate_runtime_registration(aggregators, formatters, transponders)
    
    quote do
      # Generated aggregator methods
      unquote_splicing(aggregator_methods)
      
      # Generated formatter methods
      unquote_splicing(formatter_methods)
      
      # Generated transponder methods  
      unquote_splicing(transponder_methods)
      
      # Runtime registration function
      unquote(runtime_registration)
      
      # Auto-register all components when module is loaded
      def __register_components__ do
        unquote_splicing(
          Enum.map(aggregators, fn {name, module} ->
            quote do
              Trifle.Stats.Series.Registry.register_aggregator(unquote(name), unquote(module))
            end
          end)
        )
        
        unquote_splicing(
          Enum.map(formatters, fn {name, module} ->
            quote do
              Trifle.Stats.Series.Registry.register_formatter(unquote(name), unquote(module))
            end
          end)
        )
        
        unquote_splicing(
          Enum.map(transponders, fn {name, module} ->
            quote do
              Trifle.Stats.Series.Registry.register_transponder(unquote(name), unquote(module))
            end
          end)
        )
        
        :ok
      end
    end
  end
  
  # Private helper functions for code generation
  
  defp generate_aggregator_method({name, module}) do
    method_name = :"aggregate_#{name}"
    
    quote do
      @doc """
      Generated fluent method for #{unquote(module)} aggregator.
      
      This is a terminal operation that returns raw aggregated data.
      """
      def unquote(method_name)(%Trifle.Stats.Series{} = series, path, slices \\ 1) do
        unquote(module).aggregate(series.series, path, slices)
      end
      
      # Also support additional arguments
      def unquote(method_name)(%Trifle.Stats.Series{} = series, path, slices, opts) when is_list(opts) do
        apply(unquote(module), :aggregate, [series.series, path, slices, opts])
      end
    end
  end
  
  defp generate_formatter_method({name, module}) do
    method_name = :"format_#{name}"
    
    quote do
      @doc """
      Generated fluent method for #{unquote(module)} formatter.
      
      This is a terminal operation that returns formatted data.
      """
      def unquote(method_name)(%Trifle.Stats.Series{} = series, path, slices \\ 1, transform_fn \\ nil) do
        unquote(module).format(series.series, path, slices, transform_fn)
      end
      
      # Support additional options
      def unquote(method_name)(%Trifle.Stats.Series{} = series, path, slices, transform_fn, opts) when is_list(opts) do
        apply(unquote(module), :format, [series.series, path, slices, transform_fn, opts])
      end
    end
  end
  
  defp generate_transponder_method({name, module}) do
    method_name = :"transpond_#{name}"
    
    quote do
      @doc """
      Generated fluent method for #{unquote(module)} transponder.
      
      This is an intermediate operation that returns a transformed Series.
      """
      def unquote(method_name)(%Trifle.Stats.Series{} = series, source_path, target_path, slices \\ 1) do
        updated_series = unquote(module).transform(series.series, source_path, target_path, slices)
        %Trifle.Stats.Series{series: updated_series}
      end
      
      # Support additional options
      def unquote(method_name)(%Trifle.Stats.Series{} = series, source_path, target_path, slices, opts) when is_list(opts) do
        updated_series = apply(unquote(module), :transform, [series.series, source_path, target_path, slices, opts])
        %Trifle.Stats.Series{series: updated_series}
      end
    end
  end
  
  defp generate_runtime_registration(aggregators, formatters, transponders) do
    all_components = aggregators ++ formatters ++ transponders
    
    if Enum.empty?(all_components) do
      quote do
        def __register_components__, do: :ok
      end
    else
      quote do
        def __get_registered_components__ do
          %{
            aggregators: unquote(Macro.escape(aggregators)),
            formatters: unquote(Macro.escape(formatters)),
            transponders: unquote(Macro.escape(transponders))
          }
        end
      end
    end
  end
end