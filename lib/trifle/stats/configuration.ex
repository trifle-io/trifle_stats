defmodule Trifle.Stats.Configuration do
  @moduledoc """
  Configuration management for Trifle.Stats with Ruby gem compatibility.

  Provides comprehensive configuration options including driver-specific settings,
  time zone management, granularity filtering, and Ruby gem compatibility.
  """

  defstruct [
    # Core settings (Ruby compatible)
    driver: nil,
    time_zone: "GMT",
    beginning_of_week: :monday,
    designator: nil,

    # Granularity settings (Ruby compatible)
    track_granularities: nil,  # nil means use all granularities (Ruby behavior)
    granularities: ["1m", "1h", "1d", "1w", "1mo", "1q", "1y"],

    # Driver settings
    separator: "::",
    time_zone_database: nil,

    # Driver-specific options (Ruby compatible)
    driver_options: %{},

    # Validation settings
    validate_driver: true
  ]

  @default_granularities ["1m", "1h", "1d", "1w", "1mo", "1q", "1y"]

  @doc """
  Configure Trifle.Stats with a driver and optional settings.

  ## Basic Examples
      iex> {:ok, driver} = Trifle.Stats.Driver.Process.new()
      iex> config = Trifle.Stats.Configuration.configure(driver)
      iex> config.time_zone
      "GMT"

  ## Advanced Configuration
      # Ruby-compatible configuration with driver options
      iex> {:ok, conn} = Mongo.start_link(url: "mongodb://localhost:27017/test")
      iex> config = Trifle.Stats.Configuration.configure(
      ...>   Trifle.Stats.Driver.Mongo.new(conn),
      ...>   time_zone: "Europe/London",
      ...>   track_granularities: ["1h", "1d"],
      ...>   beginning_of_week: :sunday,
      ...>   driver_options: %{
      ...>     collection_name: "analytics_stats",
      ...>     joined_identifier: true,
      ...>     expire_after: 86400  # 1 day in seconds
      ...>   }
      ...> )

  ## Driver-Specific Options

  ### MongoDB Driver Options
  - `collection_name`: Collection name (default: "trifle_stats")
  - `joined_identifier`: true/false for identifier format
  - `expire_after`: TTL in seconds for automatic expiration

  ### PostgreSQL Driver Options
  - `table_name`: Table name (default: "trifle_stats")
  - `ping_table_name`: Ping table name (default: "{table_name}_ping")
  - `joined_identifier`: true/false for table structure

  ### Redis Driver Options
  - `prefix`: Key prefix (default: "trfl")

  ### SQLite Driver Options
  - `table_name`: Table name (default: "trifle_stats")
  - `ping_table_name`: Ping table name (default: "{table_name}_ping")
  - `joined_identifier`: true/false for table structure
  """
  def configure(driver, opts \\ []) do
    # Validate driver if enabled
    if Keyword.get(opts, :validate_driver, true) do
      validate_driver!(driver)
    end

    # Core settings (Ruby compatible defaults)
    time_zone = Keyword.get(opts, :time_zone, "GMT")
    time_zone_database = Keyword.get(opts, :time_zone_database, nil)
    beginning_of_week = Keyword.get(opts, :beginning_of_week, :monday)
    track_granularities = Keyword.get(opts, :track_granularities, nil)
    designator = Keyword.get(opts, :designator, nil)
    separator = Keyword.get(opts, :separator, "::")

    # Driver-specific options
    driver_options = Keyword.get(opts, :driver_options, %{})

    # Build configuration
    config = %Trifle.Stats.Configuration{
      driver: driver,
      time_zone: time_zone,
      time_zone_database: time_zone_database,
      beginning_of_week: beginning_of_week,
      track_granularities: track_granularities,
      designator: designator,
      separator: separator,
      driver_options: driver_options,
      validate_driver: Keyword.get(opts, :validate_driver, true)
    }

    # Calculate effective granularities (Ruby behavior)
    %{config | granularities: calculate_granularities(config)}
  end

  @doc """
  Configure global application settings. This stores configuration in the Application environment
  so it can be accessed without passing config to every function call.

  ## Examples
      # In your application's config/config.exs or in an initializer
      driver = Trifle.Stats.Driver.Process.new()
      Trifle.Stats.Configuration.configure_global(
        driver: driver,
        time_zone: "Europe/London",
        track_granularities: ["1h", "1d", "1w"],
        beginning_of_week: :sunday
      )

      # Then use Trifle.Stats functions without passing config
      Trifle.Stats.track("page_views", DateTime.utc_now(), %{count: 1})
  """
  def configure_global(opts) do
    driver = Keyword.fetch!(opts, :driver)
    config = configure(driver, Keyword.delete(opts, :driver))

    Application.put_env(:trifle_stats, :global_config, config)
    config
  end

  @doc """
  Get the global configuration from Application environment.
  Returns nil if no global configuration has been set.

  ## Examples
      iex> Trifle.Stats.Configuration.get_global()
      nil

      # After configure_global has been called
      iex> config = Trifle.Stats.Configuration.get_global()
      iex> config.time_zone
      "Europe/London"
  """
  def get_global do
    Application.get_env(:trifle_stats, :global_config)
  end

  @doc """
  Clear the global configuration. Useful for testing.

  ## Examples
      iex> Trifle.Stats.Configuration.clear_global()
      :ok
  """
  def clear_global do
    Application.delete_env(:trifle_stats, :global_config)
  end

  @doc """
  Get configuration to use, preferring passed config over global config.
  If neither is provided, raises an error with helpful instructions.

  ## Examples
      iex> Trifle.Stats.Configuration.resolve_config(nil)
      ** (RuntimeError) No configuration provided and no global configuration set

      iex> config = Trifle.Stats.Configuration.configure(driver)
      iex> Trifle.Stats.Configuration.resolve_config(config)
      %Trifle.Stats.Configuration{}
  """
  def resolve_config(config) do
    case config do
      %Trifle.Stats.Configuration{} = config -> config
      nil ->
        case get_global() do
          %Trifle.Stats.Configuration{} = global_config -> global_config
          nil -> raise """
            No configuration provided and no global configuration set.

            Either:
            1. Pass a configuration to the function:
               config = Trifle.Stats.Configuration.configure(driver)
               Trifle.Stats.track("key", DateTime.utc_now(), %{count: 1}, config)

            2. Set up global configuration:
               Trifle.Stats.Configuration.configure_global(driver: driver)
               Trifle.Stats.track("key", DateTime.utc_now(), %{count: 1})
            """
        end
    end
  end

  # Keep the old API for backwards compatibility
  def configure(driver, time_zone, time_zone_database, beginning_of_week, track_granularities, separator) do
    configure(driver,
      time_zone: time_zone,
      time_zone_database: time_zone_database,
      beginning_of_week: beginning_of_week,
      track_granularities: track_granularities,
      separator: separator
    )
  end

  def set_time_zone(%Trifle.Stats.Configuration{} = configuration, time_zone) do
    %{configuration | time_zone: time_zone}
  end

  def set_time_zone_database(%Trifle.Stats.Configuration{} = configuration, time_zone_database) do
    %{configuration | time_zone_database: time_zone_database}
  end

  def set_beginning_of_week(%Trifle.Stats.Configuration{} = configuration, beginning_of_week) do
    %{configuration | beginning_of_week: beginning_of_week}
  end

  def set_granularities(%Trifle.Stats.Configuration{} = configuration, track_granularities) do
    config_with_track = %{configuration | track_granularities: track_granularities}
    %{config_with_track | granularities: calculate_granularities(config_with_track)}
  end

  def set_separator(%Trifle.Stats.Configuration{} = configuration, separator) do
    %{configuration | separator: separator}
  end

  def set_designator(%Trifle.Stats.Configuration{} = configuration, designator) do
    %{configuration | designator: designator}
  end

  ## Private Helper Functions (Ruby Compatibility)

  # Calculate effective granularities based on track_granularities (Ruby behavior)
  defp calculate_granularities(%{track_granularities: track_granularities}) do
    base_granularities = case track_granularities do
      nil -> @default_granularities  # Ruby: return all granularities if track_granularities not set
      [] -> @default_granularities   # Ruby: return all granularities if track_granularities empty
      granularities when is_list(granularities) ->
        # Convert atom granularities to string format for backward compatibility
        string_granularities = Enum.map(granularities, fn
          # :second -> "1s"
          # :minute -> "1m"
          # :hour -> "1h"
          # :day -> "1d"
          # :week -> "1w"
          # :month -> "1mo"
          # :quarter -> "1q"
          # :year -> "1y"
          str when is_binary(str) -> str
          other -> to_string(other)
        end)

        # Use custom granularities directly instead of intersecting with defaults
        string_granularities
    end

    # Filter out invalid granularities using Parser
    base_granularities
    |> Enum.uniq()
    |> Enum.filter(fn granularity ->
      parser = Trifle.Stats.Nocturnal.Parser.new(granularity)
      Trifle.Stats.Nocturnal.Parser.valid?(parser)
    end)
  end

  # Validate driver exists and has required methods (Ruby behavior)
  defp validate_driver!(driver) do
    cond do
      is_nil(driver) ->
        raise Trifle.Stats.DriverNotFoundError, "Driver cannot be nil"

      not is_driver_module?(driver) ->
        raise Trifle.Stats.DriverNotFoundError,
          "Invalid driver: #{inspect(driver)}. Must be a valid driver module."

      true ->
        :ok
    end
  end

  # Check if module is a valid driver (has required callbacks)
  defp is_driver_module?(driver) do
    # Check if it's a struct/module that looks like a driver
    case driver do
      %{__struct__: module} ->
        # Check if module has driver-like functions
        has_driver_functions?(module)
      _ ->
        false
    end
  end

  defp has_driver_functions?(module) do
    required_functions = [:inc, :set, :get, :ping, :scan]
    exported_functions = module.__info__(:functions) |> Keyword.keys()

    Enum.all?(required_functions, &(&1 in exported_functions))
  rescue
    _ -> false
  end

  @doc """
  Get timezone object compatible with Ruby TZInfo behavior.
  Returns a timezone struct that can be used for time calculations.

  ## Examples
      iex> config = %Trifle.Stats.Configuration{time_zone: "Europe/London"}
      iex> tz = Trifle.Stats.Configuration.tz(config)
      iex> tz.time_zone
      "Europe/London"

      # Invalid timezone defaults to GMT with warning
      iex> config = %Trifle.Stats.Configuration{time_zone: "Invalid/Zone"}
      iex> tz = Trifle.Stats.Configuration.tz(config)
      # Warning printed: "Trifle: Invalid timezone Invalid/Zone; Defaulting to GMT"
      iex> tz.time_zone
      "GMT"
  """
  def tz(%Trifle.Stats.Configuration{time_zone: time_zone, time_zone_database: time_zone_database}) do
    try do
      # Use configured timezone database or default
      database = time_zone_database || Tzdata.TimeZoneDatabase

      # Validate timezone
      case DateTime.now(time_zone, database) do
        {:ok, _} -> %{time_zone: time_zone, database: database}
        {:error, :time_zone_not_found} ->
          IO.puts("Trifle: Invalid timezone #{time_zone}; Defaulting to GMT.")
          %{time_zone: "GMT", database: database}
      end
    rescue
      _ ->
        IO.puts("Trifle: Error validating timezone #{time_zone}; Defaulting to GMT.")
        %{time_zone: "GMT", database: Tzdata.TimeZoneDatabase}
    end
  end

  @doc """
  Check if a value is blank (Ruby-compatible behavior).

  ## Examples
      iex> Trifle.Stats.Configuration.blank?([])
      true

      iex> Trifle.Stats.Configuration.blank?("")
      true

      iex> Trifle.Stats.Configuration.blank?(nil)
      true

      iex> Trifle.Stats.Configuration.blank?(false)
      true

      iex> Trifle.Stats.Configuration.blank?([1, 2])
      false

      iex> Trifle.Stats.Configuration.blank?("hello")
      false
  """
  def blank?(value) do
    case value do
      nil -> true
      false -> true
      [] -> true
      "" -> true
      %{} when map_size(value) == 0 -> true
      value when is_list(value) -> Enum.empty?(value)
      value when is_binary(value) -> String.trim(value) == ""
      _ -> false
    end
  end

  @doc """
  Get driver-specific options with defaults.

  ## Examples
      iex> config = %Trifle.Stats.Configuration{
      ...>   driver_options: %{collection_name: "custom_stats"}
      ...> }
      iex> Trifle.Stats.Configuration.driver_option(config, :collection_name, "trifle_stats")
      "custom_stats"

      iex> Trifle.Stats.Configuration.driver_option(config, :expire_after, nil)
      nil
  """
  def driver_option(%Trifle.Stats.Configuration{driver_options: options}, key, default \\ nil) do
    Map.get(options, key, default)
  end

  @doc """
  Merge additional driver options into configuration.

  ## Examples
      iex> config = %Trifle.Stats.Configuration{driver_options: %{}}
      iex> updated = Trifle.Stats.Configuration.merge_driver_options(config, %{
      ...>   table_name: "analytics",
      ...>   expire_after: 3600
      ...> })
      iex> updated.driver_options
      %{table_name: "analytics", expire_after: 3600}
  """
  def merge_driver_options(%Trifle.Stats.Configuration{} = config, additional_options) do
    %{config | driver_options: Map.merge(config.driver_options, additional_options)}
  end
end
