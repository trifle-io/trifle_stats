defmodule Trifle.Stats.Nocturnal.Parser do
  @moduledoc """
  Parser for granularity string representations like "1m", "15m", "33m", "1h", "1d", etc.
  
  Converts string-based granularity formats into offset and unit components.
  Handles edge cases like "33m" which creates uneven segments (0-32 and 33-59).
  
  ## Examples
  
      iex> parser = Trifle.Stats.Nocturnal.Parser.new("15m")
      iex> parser.valid?
      true
      iex> parser.offset
      15
      iex> parser.unit
      :minute
      
      # Edge case with uneven segments
      iex> parser = Trifle.Stats.Nocturnal.Parser.new("33m")
      iex> parser.valid?
      true
      iex> parser.offset
      33
      iex> parser.unit
      :minute
      
      # Invalid format
      iex> parser = Trifle.Stats.Nocturnal.Parser.new("invalid")
      iex> parser.valid?
      false
      
  """
  
  defstruct [:string, :offset, :unit]
  
  @unit_map %{
    "s" => :second,
    "m" => :minute,
    "h" => :hour,
    "d" => :day,
    "w" => :week,
    "mo" => :month,
    "q" => :quarter,
    "y" => :year
  }
  
  @doc """
  Create a new Parser instance from a granularity string.
  
  ## Examples
  
      iex> parser = Trifle.Stats.Nocturnal.Parser.new("1m")
      iex> {parser.offset, parser.unit}
      {1, :minute}
      
  """
  def new(string) do
    %__MODULE__{string: string}
    |> parse()
  end
  
  @doc """
  Check if the parsed granularity string is valid.
  
  ## Examples
  
      iex> Trifle.Stats.Nocturnal.Parser.new("15m").valid?
      true
      
      iex> Trifle.Stats.Nocturnal.Parser.new("invalid").valid?
      false
      
  """
  def valid?(%__MODULE__{offset: offset, unit: unit}) do
    !is_nil(offset) && !is_nil(unit)
  end
  
  # Private function to parse the granularity string
  defp parse(%__MODULE__{string: string} = parser) do
    case Regex.run(~r/\A(\d+)([a-z]+)\z/, string) do
      [_, offset_str, unit_str] ->
        case Map.get(@unit_map, unit_str) do
          nil -> parser
          unit ->
            offset = String.to_integer(offset_str)
            %{parser | offset: offset, unit: unit}
        end
      _ ->
        parser
    end
  end
end