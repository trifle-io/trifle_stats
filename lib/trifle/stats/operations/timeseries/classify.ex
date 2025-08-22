defmodule Trifle.Stats.Operations.Timeseries.Classify do
  @moduledoc """
  Classify operation - categorizes values using designators before incrementing.
  """
  
  alias Trifle.Stats.Nocturnal.{Parser, Key}
  
  def perform(key, at, values, config) do
    config = config || Trifle.Stats.Configuration.configure(nil, "GMT")
    
    # Generate keys for all configured granularities
    keys = 
      config.granularities
      |> Enum.map(fn granularity ->
        key_for(key, granularity, at, config)
      end)
    
    # Deep classify the values and increment
    classified_values = deep_classify(values, config)
    
    case config.driver do
      %{connection: conn} = driver when not is_nil(conn) ->
        apply(driver.__struct__, :inc, [keys, classified_values, driver])
      _ ->
        {:error, "Driver not configured"}
    end
  end
  
  defp key_for(key, granularity, at, config) do
    parser = Parser.new(granularity)
    nocturnal = Trifle.Stats.Nocturnal.new(at, config)
    floored_at = Trifle.Stats.Nocturnal.floor(nocturnal, parser.offset, parser.unit)
    Key.new(key: key, granularity: granularity, at: floored_at)
  end
  
  defp deep_classify(values, config) when is_map(values) do
    Map.new(values, fn {k, v} ->
      if is_map(v) do
        {k, deep_classify(v, config)}
      else
        classified = classify(v, config)
        {k, %{classified => 1}}
      end
    end)
  end
  
  defp deep_classify(values, _config), do: values
  
  defp classify(value, config) when is_number(value) do
    if config.designator do
      config.designator.__struct__.designate(config.designator, value)
      |> String.replace(".", "_")
    else
      "#{value}"
    end
  end
  
  defp classify(value, _config), do: "#{value}"
end