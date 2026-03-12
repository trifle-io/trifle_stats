defmodule Trifle.Stats.Series.Registry do
  @moduledoc """
  Dynamic registration system for aggregators, formatters, and transponders.

  Built-in transponders are intentionally limited to `:expression`.
  """

  use GenServer

  # Client API

  def start_link(_opts \\ []) do
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

  def call_aggregator(series, name, args) do
    case builtin_aggregator_function(name) do
      {:ok, function_name} ->
        apply(Trifle.Stats.Series, function_name, [series | args])

      {:error, :not_found} ->
        case get_aggregator(name) do
          {:ok, module} ->
            apply(module, :aggregate, [series.series | args])

          {:error, :not_found} ->
            raise ArgumentError,
                  "Unknown aggregator: #{name}. Available built-ins: #{builtin_aggregators() |> Enum.join(", ")}"
        end
    end
  end

  def call_formatter(series, name, args) do
    case builtin_formatter_function(name) do
      {:ok, function_name} ->
        apply(Trifle.Stats.Series, function_name, [series | args])

      {:error, :not_found} ->
        case get_formatter(name) do
          {:ok, module} ->
            apply(module, :format, [series.series | args])

          {:error, :not_found} ->
            raise ArgumentError,
                  "Unknown formatter: #{name}. Available built-ins: #{builtin_formatters() |> Enum.join(", ")}"
        end
    end
  end

  def call_transponder(series, name, args) do
    case builtin_transponder_function(name) do
      {:ok, function_name} ->
        apply(Trifle.Stats.Series, function_name, [series | args])

      {:error, :not_found} ->
        case get_transponder(name) do
          {:ok, module} ->
            updated_series = apply(module, :transform, [series.series | args])
            %Trifle.Stats.Series{series: updated_series}

          {:error, :not_found} ->
            raise ArgumentError,
                  "Unknown transponder: #{name}. Available built-ins: #{builtin_transponders() |> Enum.join(", ")}"
        end
    end
  end

  defp builtin_aggregator_function(:mean), do: {:ok, :aggregate_mean}
  defp builtin_aggregator_function(:max), do: {:ok, :aggregate_max}
  defp builtin_aggregator_function(:min), do: {:ok, :aggregate_min}
  defp builtin_aggregator_function(:sum), do: {:ok, :aggregate_sum}
  defp builtin_aggregator_function(_), do: {:error, :not_found}

  defp builtin_formatter_function(:category), do: {:ok, :format_category}
  defp builtin_formatter_function(:timeline), do: {:ok, :format_timeline}
  defp builtin_formatter_function(_), do: {:error, :not_found}

  defp builtin_transponder_function(:expression), do: {:ok, :transform_expression}
  defp builtin_transponder_function(_), do: {:error, :not_found}

  defp builtin_aggregators, do: [:mean, :max, :min, :sum]
  defp builtin_formatters, do: [:category, :timeline]
  defp builtin_transponders, do: [:expression]

  defp validate_aggregator!(module) do
    unless function_exported?(module, :aggregate, 2) or function_exported?(module, :aggregate, 3) do
      raise ArgumentError, "Aggregator module #{module} must implement aggregate/2 or aggregate/3"
    end
  end

  defp validate_formatter!(module) do
    unless function_exported?(module, :format, 2) or function_exported?(module, :format, 3) or
             function_exported?(module, :format, 4) do
      raise ArgumentError,
            "Formatter module #{module} must implement format/2, format/3, or format/4"
    end
  end

  defp validate_transponder!(module) do
    unless function_exported?(module, :transform, 3) or function_exported?(module, :transform, 4) or
             function_exported?(module, :transform, 5) do
      raise ArgumentError,
            "Transponder module #{module} must implement transform/3, transform/4, or transform/5"
    end
  end

  # GenServer callbacks

  @impl true
  def init(_state) do
    {:ok,
     %{
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
