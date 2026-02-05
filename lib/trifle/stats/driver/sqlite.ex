defmodule Trifle.Stats.Driver.Sqlite do
  @moduledoc """
  SQLite driver for Trifle.Stats using exqlite.
  Stores time series data in SQLite with JSON1 extension for efficient querying.
  """

  defstruct connection: nil,
            table_name: "trifle_stats",
            ping_table_name: nil,
            separator: "::",
            joined_identifier: :full,
            system_tracking: true

  def new(connection, table_name \\ "trifle_stats", ping_table_name \\ nil, joined_identifier \\ :full, system_tracking \\ true) do
    ping_table = ping_table_name || "#{table_name}_ping"
    identifier_mode = normalize_joined_identifier(joined_identifier)

    %__MODULE__{
      connection: connection,
      table_name: table_name,
      ping_table_name: ping_table,
      separator: "::",
      joined_identifier: identifier_mode,
      system_tracking: system_tracking
    }
  end

  def setup!(connection, table_name \\ "trifle_stats", joined_identifier \\ :full, ping_table_name \\ nil, system_tracking \\ true) do
    ping_table = ping_table_name || "#{table_name}_ping"
    identifier_mode = normalize_joined_identifier(joined_identifier)

    case identifier_mode do
      :full ->
        # Joined identifier mode - single table with key column
        case Exqlite.query(connection, """
          CREATE TABLE IF NOT EXISTS #{table_name} (
            key TEXT PRIMARY KEY,
            data TEXT NOT NULL DEFAULT '{}'
          )
        """, []) do
          :ok -> :ok
          {:ok, _result} -> :ok
          {:error, reason} -> raise "Failed to create main table: #{inspect(reason)}"
        end

      :partial ->
        # Partial joined mode - key + at composite primary key
        case Exqlite.query(connection, """
          CREATE TABLE IF NOT EXISTS #{table_name} (
            key TEXT NOT NULL,
            at TEXT NOT NULL,
            data TEXT NOT NULL DEFAULT '{}',
            PRIMARY KEY (key, at)
          )
        """, []) do
          :ok -> :ok
          {:ok, _result} -> :ok
          {:error, reason} -> raise "Failed to create main table: #{inspect(reason)}"
        end

      nil ->
        # Separated identifier mode - multi-column primary key
        case Exqlite.query(connection, """
          CREATE TABLE IF NOT EXISTS #{table_name} (
            key TEXT NOT NULL,
            granularity TEXT NOT NULL,
            at TEXT NOT NULL,
            data TEXT NOT NULL DEFAULT '{}',
            PRIMARY KEY (key, granularity, at)
          )
        """, []) do
          :ok -> :ok
          {:ok, _result} -> :ok
          {:error, reason} -> raise "Failed to create main table: #{inspect(reason)}"
        end
    end

    # Create ping table for ping/scan operations
    setup_ping_table!(connection, ping_table)
    :ok
  end

  defp system_identifier_for(%Trifle.Stats.Nocturnal.Key{} = key, driver) do
    system_key = %Trifle.Stats.Nocturnal.Key{
      key: "__system__key__",
      granularity: key.granularity,
      at: key.at
    }
    identifier_for(system_key, driver)
  end

  defp system_data_for(%Trifle.Stats.Nocturnal.Key{} = key, tracking_key \\ nil) do
    tracking_key = tracking_key || key.key
    Trifle.Stats.Packer.pack(%{count: 1, keys: %{tracking_key => 1}})
  end

  def setup_ping_table!(connection, ping_table_name) do
    case Exqlite.query(connection, """
      CREATE TABLE IF NOT EXISTS #{ping_table_name} (
        key TEXT PRIMARY KEY,
        at TEXT NOT NULL,
        data TEXT NOT NULL DEFAULT '{}'
      )
    """, []) do
      :ok -> :ok
      {:ok, _result} -> :ok
      {:error, reason} -> raise "Failed to create ping table: #{inspect(reason)}"
    end
  end

  def inc(keys, values, driver, tracking_key \\ nil) do
    data = Trifle.Stats.Packer.pack(values)

    # Use transaction like Ruby version for atomicity
    Exqlite.transaction(driver.connection, fn conn ->
      Enum.each(keys, fn %Trifle.Stats.Nocturnal.Key{} = key ->
        identifier = identifier_for(key, driver)
        # Batch data fields to avoid SQLite parser stack overflow
        batch_data_operations(identifier, data, driver.table_name, conn, :inc)

        # System tracking: run additional increment query with modified key and data
        if driver.system_tracking do
          system_identifier = system_identifier_for(key, driver)
          system_data = system_data_for(key, tracking_key)
          batch_data_operations(system_identifier, system_data, driver.table_name, conn, :inc)
        end
      end)
    end)
  end

  defp inc_query(identifier, data, table_name) do
    # Build SQL exactly like Ruby version with JSON functions
    columns = Map.keys(identifier) |> Enum.join(", ")
    values =
      identifier
      |> Map.values()
      |> Enum.map(&format_value/1)
      |> Enum.join(", ")
    conflict_columns = Map.keys(identifier) |> Enum.join(", ")

    # Build JSON increment operations like Ruby - use flattened keys directly
    json_increments =
      Enum.reduce(data, "data", fn {k, v}, acc ->
        # Always use the key as-is since Packer flattens everything to "key.subkey" format
        json_path = "$.\"#{k}\""  # Quote the key in case it contains dots
        "json_set(#{acc}, '#{json_path}', IFNULL(json_extract(data, '#{json_path}'), 0) + #{v})"
      end)

    """
    INSERT INTO #{table_name} (#{columns}, data) VALUES (#{values}, json('#{Jason.encode!(data)}'))
    ON CONFLICT (#{conflict_columns}) DO UPDATE SET data = #{json_increments};
    """
  end

  def set(keys, values, driver, tracking_key \\ nil) do
    data = Trifle.Stats.Packer.pack(values)

    # Use transaction like Ruby version for atomicity
    Exqlite.transaction(driver.connection, fn conn ->
      Enum.each(keys, fn %Trifle.Stats.Nocturnal.Key{} = key ->
        identifier = identifier_for(key, driver)
        # Batch data fields to avoid SQLite parser stack overflow
        batch_data_operations(identifier, data, driver.table_name, conn, :set)

        # System tracking: run additional increment query with modified key and data
        if driver.system_tracking do
          system_identifier = system_identifier_for(key, driver)
          system_data = system_data_for(key, tracking_key)
          batch_data_operations(system_identifier, system_data, driver.table_name, conn, :inc)
        end
      end)
    end)
  end

  defp set_query(identifier, data, table_name) do
    # Build SQL with complete data replacement for set operations
    columns = Map.keys(identifier) |> Enum.join(", ")
    values =
      identifier
      |> Map.values()
      |> Enum.map(&format_value/1)
      |> Enum.join(", ")
    conflict_columns = Map.keys(identifier) |> Enum.join(", ")

    # Use complete replacement instead of field-by-field for set operations
    data_json = Jason.encode!(data)

    """
    INSERT INTO #{table_name} (#{columns}, data) VALUES (#{values}, json('#{data_json}'))
    ON CONFLICT (#{conflict_columns}) DO UPDATE SET data = json('#{data_json}');
    """
  end

  def get(keys, driver) do
    # Convert keys to identifiers exactly like Ruby
    identifiers = Enum.map(keys, fn %Trifle.Stats.Nocturnal.Key{} = key ->
      identifier_for(key, driver)
    end)

    # Get data using Ruby-style get_all approach
    data = get_all(identifiers, keys, driver)

    # Map back to result order using simple_identifier for consistent lookup
    results = Enum.map(keys, fn %Trifle.Stats.Nocturnal.Key{} = key ->
      simple_identifier = simple_identifier_for(key, driver)
      raw_data = Map.get(data, simple_identifier, %{})
      Trifle.Stats.Packer.unpack(raw_data)
    end)

    results
  end

  defp get_all(identifiers, keys, driver) do
    # Build query exactly like Ruby version with OR conditions
    query = get_query(identifiers, driver.table_name)
    {:ok, result} = Exqlite.query(driver.connection, query, [])

    # Build result map using simple_identifier for consistent mapping
    Enum.reduce(result.rows, %{}, fn row, acc ->
      # Create a temporary key struct from the database row to use simple_identifier
      temp_key =
        case driver.joined_identifier do
          :full ->
            # For full joined mode, use the key as-is
            %Trifle.Stats.Nocturnal.Key{key: Enum.at(row, 0)}

          :partial ->
            # For partial joined mode, key + at
            %Trifle.Stats.Nocturnal.Key{
              key: Enum.at(row, 0),
              at: parse_timestamp_from_sqlite(Enum.at(row, 1))
            }

          nil ->
            # For separated mode, build key from individual columns
            %Trifle.Stats.Nocturnal.Key{
              key: Enum.at(row, 0),           # key column
              granularity: Enum.at(row, 1),   # granularity column
              at: parse_timestamp_from_sqlite(Enum.at(row, 2))  # at column
            }
        end

      # Use simple_identifier for consistent map key
      simple_identifier = simple_identifier_for(temp_key, driver)

      # Parse JSON data from last column (like Ruby)
      data_json = List.last(row)
      json_data = case Jason.decode(data_json) do
        {:ok, decoded} -> decoded
        {:error, _} -> %{}
      end

      Map.put(acc, simple_identifier, json_data)
    end)
  end

  # Batch data operations to avoid SQLite parser stack overflow
  # Splits large data maps into smaller chunks to prevent too many nested json_set calls
  defp batch_data_operations(identifier, data, table_name, conn, operation) do
    # SQLite can handle about 10-15 nested json_set calls safely
    batch_size = 10

    if map_size(data) <= batch_size do
      # Small data set, use single query
      query = case operation do
        :inc -> inc_query(identifier, data, table_name)
        :set -> set_query(identifier, data, table_name)
      end
      Exqlite.query!(conn, query, [])
    else
      # Large data set, split into batches
      data
      |> Enum.chunk_every(batch_size)
      |> Enum.each(fn batch ->
        batch_data = Map.new(batch)
        query = case operation do
          :inc -> inc_query(identifier, batch_data, table_name)
          :set -> set_query(identifier, batch_data, table_name)
        end
        Exqlite.query!(conn, query, [])
      end)
    end
  end

  # Helper to parse timestamp from SQLite consistently
  defp parse_timestamp_from_sqlite(timestamp_value) do
    case timestamp_value do
      time_str when is_binary(time_str) ->
        # Parse timestamp like Ruby Time.parse - ensure it matches input format
        case DateTime.from_iso8601(time_str) do
          {:ok, dt, _} -> dt
          {:error, _} ->
            # Try with Z suffix if needed
            case DateTime.from_iso8601(time_str <> "Z") do
              {:ok, dt, _} -> dt
              {:error, _} ->
                # Try parsing as integer unix timestamp
                case Integer.parse(time_str) do
                  {timestamp, ""} -> DateTime.from_unix!(timestamp)
                  _ -> time_str
                end
            end
        end
      timestamp when is_integer(timestamp) ->
        DateTime.from_unix!(timestamp)
      %DateTime{} = dt ->
        dt
      val ->
        val
    end
  end

  defp get_query(identifiers, table_name) do
    # Build OR conditions exactly like Ruby version
    conditions =
      identifiers
      |> Enum.map(&build_identifier_condition/1)
      |> Enum.join(" OR ")

    "SELECT * FROM #{table_name} WHERE #{conditions};"
  end

  defp build_identifier_condition(identifier) do
    # Build condition like Ruby version: k = v AND k = v...
    identifier
    |> Enum.map(fn {k, v} -> "#{k} = #{format_value(v)}" end)
    |> Enum.join(" AND ")
  end

  def ping(%Trifle.Stats.Nocturnal.Key{} = key, values, driver) do
    if driver.joined_identifier do
      # Return :ok like Ruby version (joined mode doesn't support ping/scan)
      :ok
    else
      # Pack data like Ruby version: { data: values, at: key.at }
      data = Trifle.Stats.Packer.pack(%{data: values, at: key.at})
      query = ping_query(key.key, key.at, data, driver.ping_table_name)

      # Use transaction like Ruby version
      case Exqlite.transaction(driver.connection, fn conn ->
        Exqlite.query!(conn, query, [])
      end) do
        {:ok, _} -> :ok
        result -> result
      end
    end
  end

  defp ping_query(key_string, at, data, ping_table_name) do
    # Format timestamp like Ruby version
    at_formatted = case at do
      %DateTime{} = dt -> Calendar.strftime(dt, "%Y-%m-%d %H:%M:%S")
      timestamp when is_integer(timestamp) ->
        timestamp |> DateTime.from_unix!() |> Calendar.strftime("%Y-%m-%d %H:%M:%S")
      _ -> raise ArgumentError, "Invalid timestamp format"
    end

    """
    INSERT INTO #{ping_table_name} (key, at, data) VALUES ('#{key_string}', '#{at_formatted}', json('#{Jason.encode!(data)}'))
    ON CONFLICT (key) DO UPDATE SET at = '#{at_formatted}', data = json('#{Jason.encode!(data)}');
    """
  end

  def scan(%Trifle.Stats.Nocturnal.Key{} = key, driver) do
    if driver.joined_identifier do
      # Return empty array like Ruby version
      []
    else
      query = scan_query(key.key, driver.ping_table_name)
      {:ok, result} = Exqlite.query(driver.connection, query, [])

      case result.rows do
        [[_key, at_string, data_json]] ->
          # Parse JSON data
          case Jason.decode(data_json) do
            {:ok, data} ->
              # Parse timestamp like Ruby's Time.parse
              {:ok, at_datetime, _offset} = DateTime.from_iso8601(at_string <> "Z")
              # Return [timestamp, data] array like Ruby version
              [at_datetime, Trifle.Stats.Packer.unpack(data)]
            {:error, _} ->
              # Return empty array like Ruby version
              []
          end
        [] ->
          # Return empty array like Ruby version
          []
      end
    end
  end

  defp scan_query(key_string, ping_table_name) do
    "SELECT key, at, data FROM #{ping_table_name} WHERE key = '#{key_string}' ORDER BY at DESC LIMIT 1;"
  end

  defp build_key(%Trifle.Stats.Nocturnal.Key{} = key, driver) do
    # For separated mode, still need string representation for some operations
    Trifle.Stats.Nocturnal.Key.join(key, driver.separator)
  end

  defp build_identifier(%Trifle.Stats.Nocturnal.Key{} = key, driver) do
    identifier_map = identifier_for(key, driver)

    if driver.joined_identifier == :full do
      identifier_map
    else
      # Convert timestamp to string for SQLite storage
      case identifier_map do
        %{at: %DateTime{} = dt} ->
          %{identifier_map | at: DateTime.to_iso8601(dt)}

        %{at: timestamp} when is_integer(timestamp) ->
          %{identifier_map | at: to_string(timestamp)}

        _ ->
          identifier_map
      end
    end
  end

  defp format_value(value) when is_binary(value), do: "'#{value}'"
  defp format_value(value) when is_integer(value), do: "#{value}"
  defp format_value(value) when is_float(value), do: "#{value}"
  defp format_value(%DateTime{} = value), do: "'#{DateTime.to_iso8601(value)}'"
  defp format_value(value), do: "'#{value}'"

  defp identifier_for(%Trifle.Stats.Nocturnal.Key{} = key, driver) do
    Trifle.Stats.Nocturnal.Key.identifier(key, driver.separator, driver.joined_identifier)
  end

  defp simple_identifier_for(%Trifle.Stats.Nocturnal.Key{} = key, driver) do
    Trifle.Stats.Nocturnal.Key.simple_identifier(key, driver.separator, driver.joined_identifier)
  end

  defp normalize_joined_identifier(nil), do: nil
  defp normalize_joined_identifier(:full), do: :full
  defp normalize_joined_identifier("full"), do: :full
  defp normalize_joined_identifier(:partial), do: :partial
  defp normalize_joined_identifier("partial"), do: :partial

  defp normalize_joined_identifier(value) do
    raise ArgumentError,
          "joined_identifier must be nil, :full, \"full\", :partial, or \"partial\", got: #{inspect(value)}"
  end
end
