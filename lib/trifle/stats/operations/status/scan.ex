defmodule Trifle.Stats.Operations.Status.Scan do
  @moduledoc """
  Scan operation - retrieves the latest status for a key.
  Returns the most recent ping data in separated identifier mode.
  """
  
  alias Trifle.Stats.Nocturnal.Key
  
  def perform(key_name, config) do
    config = config || Trifle.Stats.Configuration.configure(nil, "GMT")
    
    case config.driver do
      %{connection: conn} = driver when not is_nil(conn) ->
        # Create proper Key object for scanning
        key = Key.new(key: key_name)
        
        case apply(driver.__struct__, :scan, [key, driver]) do
          {at, values} when not is_nil(at) ->
            %{at: at, values: values}
          _ ->
            %{at: nil, values: %{}}
        end
      _ ->
        {:error, "Driver not configured"}
    end
  end
end