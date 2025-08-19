defmodule Trifle.Stats.Nocturnal.Key do
  @moduledoc """
  Key structure for trifle-stats operations.

  Represents a structured key used in time-series data storage operations.
  Keys contain the base key name, time granularity, timestamp, and optional prefix.

  ## Usage

      # Create a key with base components
      key = Trifle.Stats.Nocturnal.Key.new(key: "page_views", granularity: "hour", at: ~U[2025-08-17 10:00:00Z])
      
      # Set a prefix (usually done by drivers)
      key = Trifle.Stats.Nocturnal.Key.set_prefix(key, "stats")
      
      # Join components into a storage key
      joined = Trifle.Stats.Nocturnal.Key.join(key, "::")
      # => "stats::page_views::hour::1692266400"
      
      # Get identifier for storage (joined or separated)
      identifier = Trifle.Stats.Nocturnal.Key.identifier(key, "::")
      # => %{key: "stats::page_views::hour::1692266400"}
      
      identifier = Trifle.Stats.Nocturnal.Key.identifier(key, nil)
      # => %{key: "page_views", granularity: "hour", at: ~U[2025-08-17 10:00:00Z]}

  ## Fields

  - `:key` - The base key name (string)
  - `:granularity` - Time granularity (string or atom: "minute", "hour", "day", "week", "month", "quarter", "year")
  - `:at` - Timestamp (DateTime or integer unix timestamp)
  - `:prefix` - Optional prefix string set by drivers for namespacing
  """

  defstruct [:key, :granularity, :at, :prefix]

  @type t :: %__MODULE__{
          key: String.t(),
          granularity: String.t() | atom() | nil,
          at: DateTime.t() | integer() | nil,
          prefix: String.t() | nil
        }

  @doc """
  Creates a new Key struct.

  ## Parameters

  - `key` - The base key name (required)
  - `granularity` - Time granularity identifier (optional)
  - `at` - Timestamp for the time bucket (optional)

  ## Examples

      iex> Trifle.Stats.Nocturnal.Key.new(key: "metrics")
      %Trifle.Stats.Nocturnal.Key{key: "metrics", granularity: nil, at: nil, prefix: nil}
      
      iex> Trifle.Stats.Nocturnal.Key.new(key: "page_views", granularity: "hour", at: ~U[2025-08-17 10:00:00Z])
      %Trifle.Stats.Nocturnal.Key{key: "page_views", granularity: "hour", at: ~U[2025-08-17 10:00:00Z], prefix: nil}
  """
  @spec new(
          key: String.t(),
          granularity: String.t() | atom() | nil,
          at: DateTime.t() | integer() | nil
        ) :: t()
  def new(opts \\ []) do
    %__MODULE__{
      key: Keyword.fetch!(opts, :key),
      granularity: Keyword.get(opts, :granularity),
      at: Keyword.get(opts, :at),
      prefix: nil
    }
  end

  @doc """
  Sets a prefix on the key.

  Typically called by drivers to add their own namespace prefix.

  ## Examples

      iex> key = Trifle.Stats.Nocturnal.Key.new(key: "metrics")
      iex> Trifle.Stats.Nocturnal.Key.set_prefix(key, "stats")
      %Trifle.Stats.Nocturnal.Key{key: "metrics", granularity: nil, at: nil, prefix: "stats"}
  """
  @spec set_prefix(t(), String.t()) :: t()
  def set_prefix(%__MODULE__{} = key, prefix) do
    %{key | prefix: prefix}
  end

  @doc """
  Joins all key components into a single string using the provided separator.

  Combines prefix, key, granularity, and timestamp (as unix integer) into a single storage key.
  Only includes non-nil components.

  ## Examples

      iex> key = Trifle.Stats.Nocturnal.Key.new(key: "page_views", granularity: "hour", at: ~U[2025-08-17 10:00:00Z])
      iex> key = Trifle.Stats.Nocturnal.Key.set_prefix(key, "stats") 
      iex> Trifle.Stats.Nocturnal.Key.join(key, "::")
      "stats::page_views::hour::1692266400"
  """
  @spec join(t(), String.t()) :: String.t()
  def join(%__MODULE__{} = key, separator) do
    components = [
      key.prefix,
      key.key,
      to_string_if_present(key.granularity),
      timestamp_to_unix(key.at)
    ]

    components
    |> Enum.reject(&is_nil/1)
    |> Enum.join(separator)
  end

  @doc """
  Returns an identifier suitable for driver storage.

  If separator is provided, returns a joined string key.
  If separator is nil, returns a map with separate key components.

  ## Examples

      # Joined mode (for drivers that store keys as single strings)
      iex> key = Trifle.Stats.Nocturnal.Key.new(key: "page_views", granularity: "hour", at: ~U[2025-08-17 10:00:00Z])
      iex> Trifle.Stats.Nocturnal.Key.identifier(key, "::")
      %{key: "page_views::hour::1692266400"}
      
      # Separated mode (for drivers that store key components separately)
      iex> key = Trifle.Stats.Nocturnal.Key.new(key: "page_views", granularity: "hour", at: ~U[2025-08-17 10:00:00Z])
      iex> Trifle.Stats.Nocturnal.Key.identifier(key, nil)
      %{key: "page_views", granularity: "hour", at: ~U[2025-08-17 10:00:00Z]}
  """
  @spec identifier(t(), String.t() | nil) :: map()
  def identifier(%__MODULE__{} = key, separator) when is_binary(separator) do
    %{key: join(key, separator)}
  end

  def identifier(%__MODULE__{} = key, nil) do
    %{key: key.key, granularity: key.granularity, at: key.at}
    |> Enum.reject(fn {_k, v} -> is_nil(v) end)
    |> Map.new()
  end

  @doc """
  Returns a simplified identifier for consistent map key lookup.

  Always converts timestamps to unix integers for consistent mapping,
  regardless of the original timestamp format. Used internally by drivers
  for result mapping and lookup operations.

  ## Examples

      # Joined mode
      iex> key = Trifle.Stats.Nocturnal.Key.new(key: "page_views", granularity: "hour", at: ~U[2025-08-17 10:00:00Z])
      iex> Trifle.Stats.Nocturnal.Key.simple_identifier(key, "::")
      %{key: "page_views::hour::1692266400"}
      
      # Separated mode
      iex> key = Trifle.Stats.Nocturnal.Key.new(key: "page_views", granularity: "hour", at: ~U[2025-08-17 10:00:00Z])
      iex> Trifle.Stats.Nocturnal.Key.simple_identifier(key, nil)
      %{key: "page_views", granularity: "hour", at: 1692266400}
  """
  @spec simple_identifier(t(), String.t() | nil) :: map()
  def simple_identifier(%__MODULE__{} = key, separator) when is_binary(separator) do
    %{key: join(key, separator)}
  end

  def simple_identifier(%__MODULE__{} = key, nil) do
    %{key: key.key, granularity: key.granularity, at: timestamp_to_unix(key.at)}
    |> Enum.reject(fn {_k, v} -> is_nil(v) end)
    |> Map.new()
  end

  # Helper to convert values to strings only if present
  defp to_string_if_present(nil), do: nil
  defp to_string_if_present(value), do: to_string(value)

  # Helper to convert DateTime to unix timestamp
  defp timestamp_to_unix(nil), do: nil
  defp timestamp_to_unix(%DateTime{} = dt), do: DateTime.to_unix(dt)
  defp timestamp_to_unix(timestamp) when is_integer(timestamp), do: timestamp
end
