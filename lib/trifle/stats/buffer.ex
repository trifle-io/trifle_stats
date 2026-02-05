defmodule Trifle.Stats.Buffer do
  @moduledoc """
  Buffered write layer for Trifle.Stats.

  Queues write operations (increment and set) and flushes them either when the
  queue reaches a configurable size or after a configurable duration. Supports
  optional aggregation, combining repeated operations on the same key set.
  """

  use GenServer

  defstruct [:pid]

  @type t :: %__MODULE__{pid: pid()}

  @default_duration 1.0
  @registry_table :trifle_stats_buffer_registry
  @at_exit_flag_key {__MODULE__, :at_exit_registered}

  @on_load :init_module
  @default_size 256

  def init_module do
    ensure_registry()
    maybe_register_at_exit()
    :ok
  end

  # Public API -----------------------------------------------------------------

  @spec new(keyword) :: t()
  def new(opts) do
    {:ok, pid} = start_link(opts)
    %__MODULE__{pid: pid}
  end

  @spec start_link(keyword) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  def inc(keys, values, %__MODULE__{pid: pid}, tracking_key \\ nil) do
    GenServer.call(pid, {:enqueue, :inc, keys, values, tracking_key}, :infinity)
  end

  def set(keys, values, %__MODULE__{pid: pid}, tracking_key \\ nil) do
    GenServer.call(pid, {:enqueue, :set, keys, values, tracking_key}, :infinity)
  end

  def flush(%__MODULE__{pid: pid}) do
    GenServer.call(pid, :flush, :infinity)
  end

  def shutdown(%__MODULE__{pid: pid}) do
    GenServer.call(pid, :shutdown, :infinity)
  end

  def flush_all do
    ensure_registry()

    buffers = :ets.tab2list(@registry_table)
    Enum.each(buffers, fn {pid} -> safe_shutdown(pid) end)
  end

  # GenServer callbacks --------------------------------------------------------

  @impl true
  def init(opts) do
    driver = Keyword.fetch!(opts, :driver)
    aggregate = Keyword.get(opts, :aggregate, true)
    size = Keyword.get(opts, :size, @default_size) |> normalize_size()
    duration = Keyword.get(opts, :duration, @default_duration) |> normalize_duration()
    async = Keyword.get(opts, :async, true)

    state = %{
      driver: driver,
      aggregate: aggregate,
      size: size,
      duration: duration,
      async: async,
      queue: new_queue(aggregate),
      operation_count: 0,
      timer_ref: nil
    }

    register_buffer(self())
    maybe_register_at_exit()
    state = maybe_schedule_flush(state)
    {:ok, state}
  end

  @impl true
  def handle_call({:enqueue, operation, keys, values, tracking_key}, _from, state) do
    new_state = handle_enqueue(operation, keys, values, tracking_key, state)
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call(:flush, _from, state) do
    state = flush_queue(state)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call(:shutdown, _from, state) do
    state = flush_queue(cancel_timer(state))
    unregister_buffer(self())
    {:stop, :normal, :ok, state}
  end

  @impl true
  def handle_cast({:enqueue, operation, keys, values, tracking_key}, state) do
    new_state = handle_enqueue(operation, keys, values, tracking_key, state)
    {:noreply, new_state}
  end

  @impl true
  def handle_info(:flush_tick, state) do
    state =
      state
      |> flush_queue()
      |> maybe_schedule_flush()

    {:noreply, state}
  end

  @impl true
  def terminate(_reason, state) do
    flush_queue(state)

    queue_size =
      case state.queue do
        %{} -> map_size(state.queue)
        list when is_list(list) -> length(list)
      end

    unregister_buffer(self())
    :ok
  end

  # Internal helpers -----------------------------------------------------------

  defp handle_enqueue(operation, keys, values, tracking_key, state) do
    new_state =
      state
      |> store_action(operation, keys, values, tracking_key)
      |> increment_operation_count()
      |> flush_when_threshold_reached()

    new_state
  end

  defp store_action(%{aggregate: true, queue: queue} = state, operation, keys, values, tracking_key) do
    signature = signature_for(operation, keys, tracking_key)

    updated_queue =
      case Map.get(queue, signature) do
        nil ->
          Map.put(queue, signature, %{operation: operation, keys: keys, values: values, tracking_key: tracking_key})

        %{values: existing_values} = entry ->
          merged_values = merge_values(operation, existing_values, values)
          Map.put(queue, signature, %{entry | values: merged_values})
      end

    %{state | queue: updated_queue}
  end

  defp store_action(%{aggregate: false, queue: queue} = state, operation, keys, values, tracking_key) do
    action = %{operation: operation, keys: keys, values: values, tracking_key: tracking_key}
    %{state | queue: [action | queue]}
  end

  defp increment_operation_count(state) do
    %{state | operation_count: state.operation_count + 1}
  end

  defp flush_when_threshold_reached(%{operation_count: count, size: size} = state)
       when count >= size do
    state
    |> cancel_timer()
    |> flush_queue()
    |> maybe_schedule_flush()
  end

  defp flush_when_threshold_reached(state), do: state

  defp flush_queue(%{operation_count: 0} = state), do: state

  defp flush_queue(state) do
    {actions, new_queue} = drain_queue(state.queue, state.aggregate)

    # Skip if no actions to flush
    if Enum.empty?(actions) do
      state
    else
      log_flush(state, actions)

      driver = state.driver
      Enum.each(actions, fn action -> dispatch_action(action, driver) end)

      %{state | queue: new_queue, operation_count: 0}
    end
  end

  defp dispatch_action(action, driver) do
    module = driver.__struct__
    apply(module, action.operation, [action.keys, action.values, driver, action.tracking_key])
  end

  defp log_flush(_state, _actions), do: :ok

  defp drain_queue(queue, true), do: {Map.values(queue), %{}}
  defp drain_queue(queue, false), do: {Enum.reverse(queue), []}

  defp merge_values(:inc, current, incoming), do: merge_increment(current, incoming)
  defp merge_values(:set, _current, incoming), do: incoming
  defp merge_values(_operation, _current, incoming), do: incoming

  defp merge_increment(current, incoming) do
    Enum.reduce(incoming, current, fn {key, value}, acc ->
      cond do
        is_map(value) ->
          nested = Map.get(acc, key, %{})
          Map.put(acc, key, merge_increment(nested, value))

        is_number(value) ->
          Map.put(acc, key, Map.get(acc, key, 0) + value)

        true ->
          Map.put(acc, key, value)
      end
    end)
  end

  defp signature_for(operation, keys, tracking_key) do
    tracking_marker = tracking_key || "__tracked__"
    identifiers =
      Enum.map(keys, fn key ->
        [
          Map.get(key, :prefix),
          Map.get(key, :key),
          Map.get(key, :granularity),
          timestamp_identifier(Map.get(key, :at))
        ]
        |> Enum.map(&to_string_if_present/1)
        |> Enum.reject(&is_nil/1)
        |> Enum.join(":")
      end)

    Enum.join([operation, tracking_marker | identifiers], "|")
  end

  defp timestamp_identifier(nil), do: nil
  defp timestamp_identifier(%DateTime{} = dt), do: DateTime.to_unix(dt)
  defp timestamp_identifier(value) when is_integer(value), do: value

  defp to_string_if_present(nil), do: nil
  defp to_string_if_present(value), do: to_string(value)

  defp new_queue(true), do: %{}
  defp new_queue(false), do: []

  defp normalize_size(size) when is_integer(size) and size > 0, do: size
  defp normalize_size(size) when is_float(size), do: trunc(Float.ceil(size)) |> max(1)
  defp normalize_size(_size), do: 1

  defp normalize_duration(nil), do: 0
  defp normalize_duration(duration) when duration <= 0, do: 0
  defp normalize_duration(duration) when is_integer(duration), do: duration * 1000
  defp normalize_duration(duration) when is_float(duration), do: trunc(duration * 1000)
  defp normalize_duration(_duration), do: 0

  defp maybe_schedule_flush(%{async: true, duration: duration} = state) when duration > 0 do
    ref = Process.send_after(self(), :flush_tick, duration)
    %{state | timer_ref: ref}
  end

  defp maybe_schedule_flush(state), do: %{state | timer_ref: nil}

  defp cancel_timer(%{timer_ref: nil} = state), do: state

  defp cancel_timer(%{timer_ref: ref} = state) do
    Process.cancel_timer(ref)
    %{state | timer_ref: nil}
  end

  defp maybe_register_at_exit do
    cond do
      :persistent_term.get(@at_exit_flag_key, false) -> :ok
      Process.whereis(:elixir_config) ->
        :persistent_term.put(@at_exit_flag_key, true)
        System.at_exit(fn _ -> flush_all() end)
        :ok

      true ->
        :ok
    end
  end

  defp register_buffer(pid) do
    ensure_registry()
    :ets.insert(@registry_table, {pid})
  end

  defp unregister_buffer(pid) do
    ensure_registry()
    :ets.delete(@registry_table, pid)
  end

  defp ensure_registry do
    case :ets.whereis(@registry_table) do
      :undefined ->
        try do
          :ets.new(@registry_table, [:named_table, :public, :set])
        rescue
          ArgumentError -> :ok
        end

      _tid ->
        :ok
    end
  end

  defp safe_shutdown(pid) when is_pid(pid) do
    if Process.alive?(pid) do
      try do
        GenServer.call(pid, :shutdown, :infinity)
      catch
        _, _ -> :ok
      end
    else
      :ok
    end
  end
end
