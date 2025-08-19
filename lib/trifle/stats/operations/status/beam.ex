defmodule Trifle.Stats.Operations.Status.Beam do
  @moduledoc """
  Beam operation - sends a status ping with current data.
  Used for tracking latest values in separated identifier mode.
  """
  
  alias Trifle.Stats.Nocturnal.Key
  
  def perform(key_name, at, values, config) do
    config = config || Trifle.Stats.Configuration.configure(nil, "GMT")
    
    case config.driver do
      %{connection: conn} = driver when not is_nil(conn) ->
        # Create proper Key object with timestamp
        key = Key.new(key: key_name, at: at)
        apply(driver.__struct__, :ping, [key, values, driver])
      _ ->
        {:error, "Driver not configured"}
    end
  end
end