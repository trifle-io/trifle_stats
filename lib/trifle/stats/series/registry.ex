defmodule Trifle.Stats.Series.Registry do
  @moduledoc """
  Dynamic registration system for aggregators, formatters, and transponders.
  
  Allows runtime registration of custom implementations while maintaining
  performance of built-in components through static method dispatch.
  
  ## Usage
  
      # Register a custom aggregator
      defmodule MyCustomAggregator do
        @behaviour Trifle.Stats.Aggregator.Behaviour
        
        def aggregate(series, path, opts \\ []) do
          # Custom aggregation logic
          []
        end
      end
      
      # Register it
      Trifle.Stats.Series.Registry.register_aggregator(:my_custom, MyCustomAggregator)
      
      # Use it
      series.aggregate.my_custom("path")
      # or
      Trifle.Stats.Series.call_aggregator(series, :my_custom, ["path"])
  """
  
  use GenServer
  
  # Client API
  
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end
  
  @doc """
  Register a custom aggregator module.
  
  The module must implement the Trifle.Stats.Aggregator.Behaviour.
  """
  def register_aggregator(name, module) when is_atom(name) and is_atom(module) do
    validate_aggregator!(module)
    GenServer.call(__MODULE__, {:register, :aggregators, name, module})
  end
  
  @doc """
  Register a custom formatter module.
  
  The module must implement the Trifle.Stats.Formatter.Behaviour.
  """
  def register_formatter(name, module) when is_atom(name) and is_atom(module) do
    validate_formatter!(module)
    GenServer.call(__MODULE__, {:register, :formatters, name, module})
  end
  
  @doc """
  Register a custom transponder module.
  
  The module must implement the Trifle.Stats.Transponder.Behaviour.
  """
  def register_transponder(name, module) when is_atom(name) and is_atom(module) do
    validate_transponder!(module)
    GenServer.call(__MODULE__, {:register, :transponders, name, module})
  end
  
  @doc """
  Get a registered aggregator module by name.
  """
  def get_aggregator(name) do
    GenServer.call(__MODULE__, {:get, :aggregators, name})
  end
  
  @doc """
  Get a registered formatter module by name.
  """
  def get_formatter(name) do
    GenServer.call(__MODULE__, {:get, :formatters, name})
  end
  
  @doc """
  Get a registered transponder module by name.
  """
  def get_transponder(name) do
    GenServer.call(__MODULE__, {:get, :transponders, name})
  end
  
  @doc """
  List all registered components.
  """
  def list_all do
    GenServer.call(__MODULE__, :list_all)
  end
  
  @doc """
  Clear all registered components (useful for testing).
  """
  def clear_all do
    GenServer.call(__MODULE__, :clear_all)
  end
  
  # Built-in component lookup (fallback to registry if not found in statics)
  
  @doc """
  Call an aggregator by name, checking built-ins first, then registry.
  """
  def call_aggregator(series_aggregator, name, args) do
    case get_builtin_aggregator(name) do
      {:ok, function} ->
        apply(function, [series_aggregator | args])
      {:error, :not_found} ->
        case get_aggregator(name) do
          {:ok, module} ->
            apply(module, :aggregate, [series_aggregator.series.series | args])
          {:error, :not_found} ->
            raise ArgumentError, "Unknown aggregator: #{name}. Available built-ins: #{builtin_aggregators() |> Enum.join(", ")}"
        end
    end
  end
  
  @doc """
  Call a formatter by name, checking built-ins first, then registry.
  """
  def call_formatter(series_formatter, name, args) do
    case get_builtin_formatter(name) do
      {:ok, function} ->
        apply(function, [series_formatter | args])
      {:error, :not_found} ->
        case get_formatter(name) do
          {:ok, module} ->
            apply(module, :format, [series_formatter.series.series | args])
          {:error, :not_found} ->
            raise ArgumentError, "Unknown formatter: #{name}. Available built-ins: #{builtin_formatters() |> Enum.join(", ")}"
        end
    end
  end
  
  @doc """
  Call a transponder by name, checking built-ins first, then registry.
  """
  def call_transponder(series_transponder, name, args) do
    case get_builtin_transponder(name) do
      {:ok, function} ->
        apply(function, [series_transponder | args])
      {:error, :not_found} ->
        case get_transponder(name) do
          {:ok, module} ->
            updated_series = apply(module, :transform, [series_transponder.series.series | args])
            %Trifle.Stats.Series{series: updated_series}
          {:error, :not_found} ->
            raise ArgumentError, "Unknown transponder: #{name}. Available built-ins: #{builtin_transponders() |> Enum.join(", ")}"
        end
    end
  end
  
  # Private helpers for built-in component lookup
  
  defp get_builtin_aggregator(:avg), do: {:ok, &Trifle.Stats.Series.Aggregator.avg/2}
  defp get_builtin_aggregator(:max), do: {:ok, &Trifle.Stats.Series.Aggregator.max/2}
  defp get_builtin_aggregator(:min), do: {:ok, &Trifle.Stats.Series.Aggregator.min/2}
  defp get_builtin_aggregator(:sum), do: {:ok, &Trifle.Stats.Series.Aggregator.sum/2}
  defp get_builtin_aggregator(_), do: {:error, :not_found}
  
  defp get_builtin_formatter(:category), do: {:ok, &Trifle.Stats.Series.Formatter.category/2}
  defp get_builtin_formatter(:timeline), do: {:ok, &Trifle.Stats.Series.Formatter.timeline/2}
  defp get_builtin_formatter(_), do: {:error, :not_found}
  
  defp get_builtin_transponder(:average), do: {:ok, &Trifle.Stats.Series.Transponder.average/3}
  defp get_builtin_transponder(:ratio), do: {:ok, &Trifle.Stats.Series.Transponder.ratio/3}
  defp get_builtin_transponder(:standard_deviation), do: {:ok, &Trifle.Stats.Series.Transponder.standard_deviation/3}
  defp get_builtin_transponder(_), do: {:error, :not_found}
  
  defp builtin_aggregators, do: [:avg, :max, :min, :sum]
  defp builtin_formatters, do: [:category, :timeline]
  defp builtin_transponders, do: [:average, :ratio, :standard_deviation]
  
  # Validation helpers
  
  defp validate_aggregator!(module) do
    unless function_exported?(module, :aggregate, 2) or function_exported?(module, :aggregate, 3) do
      raise ArgumentError, "Aggregator module #{module} must implement aggregate/2 or aggregate/3"
    end
  end
  
  defp validate_formatter!(module) do
    unless function_exported?(module, :format, 2) or function_exported?(module, :format, 3) or function_exported?(module, :format, 4) do
      raise ArgumentError, "Formatter module #{module} must implement format/2, format/3, or format/4"
    end
  end
  
  defp validate_transponder!(module) do
    unless function_exported?(module, :transform, 3) or function_exported?(module, :transform, 4) or function_exported?(module, :transform, 5) do
      raise ArgumentError, "Transponder module #{module} must implement transform/3, transform/4, or transform/5"
    end
  end
  
  # GenServer callbacks
  
  @impl true
  def init(state) do
    {:ok, %{
      aggregators: %{},
      formatters: %{},
      transponders: %{}
    }}
  end
  
  @impl true
  def handle_call({:register, type, name, module}, _from, state) do
    updated_state = put_in(state, [type, name], module)
    {:reply, :ok, updated_state}
  end
  
  @impl true
  def handle_call({:get, type, name}, _from, state) do
    case get_in(state, [type, name]) do
      nil -> {:reply, {:error, :not_found}, state}
      module -> {:reply, {:ok, module}, state}
    end
  end
  
  @impl true
  def handle_call(:list_all, _from, state) do
    {:reply, state, state}
  end
  
  @impl true
  def handle_call(:clear_all, _from, _state) do
    {:reply, :ok, %{aggregators: %{}, formatters: %{}, transponders: %{}}}
  end
end