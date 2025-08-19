defmodule Trifle.Stats.Driver.Process do
  @moduledoc """
  In-memory Process driver for Trifle.Stats.
  
  This driver stores all data in memory using a GenServer process.
  It's designed for development, testing, and scenarios where persistence
  is not required. Data is lost when the process terminates.
  
  ## Usage
  
      # Start the driver (can be supervised)
      {:ok, pid} = Trifle.Stats.Driver.Process.start_link()
      driver = Trifle.Stats.Driver.Process.new(pid)
      
      # Use with configuration
      config = Trifle.Stats.Configuration.configure(driver)
  """
  
  use GenServer
  
  defstruct connection: nil, separator: "::"
  
  def new(connection, separator \\ "::") do
    %__MODULE__{
      connection: connection,
      separator: separator
    }
  end
  
  @doc """
  Starts the Process driver GenServer.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, %{data: %{}, status: %{}}, opts)
  end
  
  @doc """
  Description of the driver for debugging/logging purposes.
  """
  def description(driver) do
    "#{__MODULE__}(J) - PID #{inspect(driver.connection)}"
  end
  
  def inc(keys, values, driver) do
    data = Trifle.Stats.Packer.pack(values)
    
    Enum.each(keys, fn %Trifle.Stats.Nocturnal.Key{} = key ->
      packed_key = Trifle.Stats.Nocturnal.Key.join(key, driver.separator)
      
      GenServer.call(driver.connection, {:inc, packed_key, data})
    end)
  end
  
  def set(keys, values, driver) do
    data = Trifle.Stats.Packer.pack(values)
    
    Enum.each(keys, fn %Trifle.Stats.Nocturnal.Key{} = key ->
      packed_key = Trifle.Stats.Nocturnal.Key.join(key, driver.separator)
      
      GenServer.call(driver.connection, {:set, packed_key, data})
    end)
  end
  
  def get(keys, driver) do
    Enum.map(keys, fn %Trifle.Stats.Nocturnal.Key{} = key ->
      packed_key = Trifle.Stats.Nocturnal.Key.join(key, driver.separator)
      
      case GenServer.call(driver.connection, {:get, packed_key}) do
        nil -> %{}
        data -> Trifle.Stats.Packer.unpack(data)
      end
    end)
  end
  
  def ping(%Trifle.Stats.Nocturnal.Key{} = key, values, driver) do
    # Use base key without prefix for ping operations (like other drivers)
    packed_data = Trifle.Stats.Packer.pack(%{data: values, at: key.at})
    
    GenServer.call(driver.connection, {:ping, key.key, packed_data})
    :ok
  end
  
  def scan(%Trifle.Stats.Nocturnal.Key{} = key, driver) do
    # Use base key without prefix for scan operations
    case GenServer.call(driver.connection, {:scan, key.key}) do
      nil -> [nil, %{}]
      {at, data} -> [at, Trifle.Stats.Packer.unpack(data)]
    end
  end
  
  # GenServer callbacks
  
  @impl true
  def init(state) do
    {:ok, state}
  end
  
  @impl true
  def handle_call({:inc, key, data}, _from, state) do
    existing_data = Map.get(state.data, key, %{})
    
    # Merge with incremental addition
    new_data = Map.merge(existing_data, data, fn _k, v1, v2 ->
      case {v1, v2} do
        {a, b} when is_number(a) and is_number(b) -> a + b
        {_, b} -> b  # Replace non-numeric values
      end
    end)
    
    updated_state = put_in(state, [:data, key], new_data)
    {:reply, :ok, updated_state}
  end
  
  @impl true
  def handle_call({:set, key, data}, _from, state) do
    # Replace existing data completely
    updated_state = put_in(state, [:data, key], data)
    {:reply, :ok, updated_state}
  end
  
  @impl true
  def handle_call({:get, key}, _from, state) do
    data = Map.get(state.data, key)
    {:reply, data, state}
  end
  
  @impl true
  def handle_call({:ping, key, data}, _from, state) do
    # Store ping data with timestamp
    at = case data["at"] do
      timestamp when is_number(timestamp) -> DateTime.from_unix!(timestamp)
      _ -> DateTime.utc_now()
    end
    
    updated_state = put_in(state, [:status, key], {at, data})
    {:reply, :ok, updated_state}
  end
  
  @impl true
  def handle_call({:scan, key}, _from, state) do
    result = Map.get(state.status, key)
    {:reply, result, state}
  end
  
  @impl true
  def handle_call({:clear}, _from, _state) do
    # Clear all data - useful for testing
    {:reply, :ok, %{data: %{}, status: %{}}}
  end
  
  @impl true
  def handle_call({:debug_state}, _from, state) do
    {:reply, state, state}
  end
  
  @doc """
  Clears all stored data. Useful for testing.
  """
  def clear(driver) do
    GenServer.call(driver.connection, {:clear})
  end
  
  @doc """
  Returns the current state for debugging. Not part of the driver interface.
  """
  def debug_state(driver) do
    GenServer.call(driver.connection, {:debug_state})
  end
end