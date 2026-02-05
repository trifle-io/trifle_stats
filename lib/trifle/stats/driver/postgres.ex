defmodule Trifle.Stats.Driver.Postgres do
  @moduledoc """
  PostgreSQL driver for Trifle.Stats using JSONB columns for efficient JSON storage.
  Supports both joined and separated identifier modes.
  """

  defstruct connection: nil,
            table_name: "trifle_stats",
            ping_table_name: nil,
            separator: "::",
            joined_identifier: :full,
            system_tracking: true

  def new(
        connection,
        table_name \\ "trifle_stats",
        joined_identifier \\ :full,
        ping_table_name \\ nil,
        system_tracking \\ true
      ) do
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

  def setup!(
        connection,
        table_name \\ "trifle_stats",
        joined_identifier \\ :full,
        ping_table_name \\ nil,
        system_tracking \\ true
      ) do
    ping_table = ping_table_name || "#{table_name}_ping"
    identifier_mode = normalize_joined_identifier(joined_identifier)

    case identifier_mode do
      :full ->
        # Joined identifier mode - single table with key column
        Postgrex.query!(
          connection,
          """
            CREATE TABLE IF NOT EXISTS #{table_name} (
              key VARCHAR(255) PRIMARY KEY,
              data JSONB NOT NULL DEFAULT '{}'::jsonb
            )
          """,
          []
        )

      :partial ->
        # Partial joined mode - key + at composite primary key
        Postgrex.query!(
          connection,
          """
            CREATE TABLE IF NOT EXISTS #{table_name} (
              key VARCHAR(255) NOT NULL,
              at TIMESTAMPTZ NOT NULL,
              data JSONB NOT NULL DEFAULT '{}'::jsonb,
              PRIMARY KEY (key, at)
            )
          """,
          []
        )

      nil ->
        # Separated identifier mode - multi-column primary key
        Postgrex.query!(
          connection,
          """
            CREATE TABLE IF NOT EXISTS #{table_name} (
              key VARCHAR(255) NOT NULL,
              granularity VARCHAR(255) NOT NULL,
              at TIMESTAMPTZ NOT NULL,
              data JSONB NOT NULL DEFAULT '{}'::jsonb,
              PRIMARY KEY (key, granularity, at)
            )
          """,
          []
        )

        # Create ping table for separated mode
        Postgrex.query!(
          connection,
          """
            CREATE TABLE IF NOT EXISTS #{ping_table} (
              key VARCHAR(255) PRIMARY KEY,
              at TIMESTAMPTZ NOT NULL,
              data JSONB NOT NULL DEFAULT '{}'::jsonb
            )
          """,
          []
        )
    end

    # Create ping table for joined modes
    if identifier_mode in [:full, :partial] do
      setup_ping_table!(connection, ping_table)
    end

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
    Postgrex.query!(
      connection,
      """
        CREATE TABLE IF NOT EXISTS #{ping_table_name} (
          key VARCHAR(255) PRIMARY KEY,
          at TIMESTAMPTZ NOT NULL,
          data JSONB NOT NULL DEFAULT '{}'::jsonb
        )
      """,
      []
    )

    :ok
  end

  def inc(keys, values, driver, tracking_key \\ nil) do
    data = Trifle.Stats.Packer.pack(values)

    Postgrex.transaction(driver.connection, fn conn ->
      Enum.each(keys, fn %Trifle.Stats.Nocturnal.Key{} = key ->
        # Use the raw identifier directly without modification
        identifier = identifier_for(key, driver)
        query = inc_query(identifier, data, driver.table_name)
        Postgrex.query!(conn, query, [])

        # System tracking: run additional increment query with modified key and data
        if driver.system_tracking do
          system_identifier = system_identifier_for(key, driver)
          system_data = system_data_for(key, tracking_key)
          system_query = inc_query(system_identifier, system_data, driver.table_name)
          Postgrex.query!(conn, system_query, [])
        end
      end)
    end)
  end

  def set(keys, values, driver, tracking_key \\ nil) do
    data = Trifle.Stats.Packer.pack(values)

    Postgrex.transaction(driver.connection, fn conn ->
      Enum.each(keys, fn %Trifle.Stats.Nocturnal.Key{} = key ->
        # Use the raw identifier directly without modification
        identifier = identifier_for(key, driver)
        query = set_query(identifier, data, driver.table_name)
        Postgrex.query!(conn, query, [])

        # System tracking: run additional increment query with modified key and data
        if driver.system_tracking do
          system_identifier = system_identifier_for(key, driver)
          system_data = system_data_for(key, tracking_key)
          system_query = inc_query(system_identifier, system_data, driver.table_name)
          Postgrex.query!(conn, system_query, [])
        end
      end)
    end)
  end

  def get(keys, driver) do
    identifiers =
      Enum.map(keys, fn %Trifle.Stats.Nocturnal.Key{} = key ->
        identifier_for(key, driver)
      end)

    # Get all data from database with Ruby-style OR query
    data = get_all(identifiers, keys, driver)

    # Map back to result order using simple_identifier for consistent lookup
    Enum.map(keys, fn %Trifle.Stats.Nocturnal.Key{} = key ->
      simple_identifier = simple_identifier_for(key, driver)
      raw_data = Map.get(data, simple_identifier, %{})
      Trifle.Stats.Packer.unpack(raw_data)
    end)
  end

  def ping(%Trifle.Stats.Nocturnal.Key{} = key, values, driver) do
    if driver.joined_identifier do
      # Return :ok like Ruby version (joined mode doesn't support ping/scan)
      :ok
    else
      # Use base key without prefix/separator for ping operations (like Ruby)
      data = Trifle.Stats.Packer.pack(%{data: values, at: key.at})
      query = ping_query(key.key, key.at, data, driver.ping_table_name)

      # Use transaction like Ruby version
      case Postgrex.transaction(driver.connection, fn conn ->
             Postgrex.query!(conn, query, [])
           end) do
        {:ok, _} -> :ok
        result -> result
      end
    end
  end

  def scan(%Trifle.Stats.Nocturnal.Key{} = key, driver) do
    if driver.joined_identifier do
      # Return empty array like Ruby version
      []
    else
      # Use base key without prefix/separator for scan operations (like Ruby)
      query = scan_query(key.key, driver.ping_table_name)
      result = Postgrex.query!(driver.connection, query, [])

      case result.rows do
        [[at_datetime, data_json]] ->
          # Handle both string and already-decoded JSONB data
          data =
            case data_json do
              str when is_binary(str) ->
                case Jason.decode(str) do
                  {:ok, decoded} -> decoded
                  {:error, _} -> %{}
                end

              map when is_map(map) ->
                map

              _ ->
                %{}
            end

          # Return [timestamp, data] array like Ruby version
          [at_datetime, Trifle.Stats.Packer.unpack(data)]

        _ ->
          # Return empty array like Ruby version
          []
      end
    end
  end

  defp get_all(identifiers, keys, driver) do
    # Build query exactly like Ruby version with OR conditions
    query = get_query(identifiers, driver.table_name)
    result = Postgrex.query!(driver.connection, query, [])

    # Build a map from database rows to their corresponding original keys
    key_map = build_key_mapping(keys, driver)

    # Build result map using simple_identifier for consistent mapping
    Enum.reduce(result.rows, %{}, fn row, acc ->
      # Extract columns and build identifier like Ruby
      data_map = build_row_map(row, result.columns)

      # Create a temporary key struct from the database row to use simple_identifier
      temp_key = %Trifle.Stats.Nocturnal.Key{
        key: data_map["key"],
        granularity: data_map["granularity"],
        at: parse_timestamp_from_db(data_map["at"])
      }

      # Use simple_identifier for consistent map key
      simple_identifier = simple_identifier_for(temp_key, driver)

      # Parse JSON data
      json_data =
        case data_map["data"] do
          str when is_binary(str) ->
            case Jason.decode(str) do
              {:ok, decoded} -> decoded
              {:error, _} -> %{}
            end

          map when is_map(map) ->
            map

          _ ->
            %{}
        end

      Map.put(acc, simple_identifier, json_data)
    end)
  end

  # Helper to build mapping from original keys for lookup
  defp build_key_mapping(keys, driver) do
    Enum.map(keys, fn key ->
      {simple_identifier_for(key, driver), key}
    end)
    |> Map.new()
  end

  # Helper to parse timestamp from database consistently
  defp parse_timestamp_from_db(timestamp_value) do
    case timestamp_value do
      time_str when is_binary(time_str) ->
        # Parse timestamp from PostgreSQL
        {:ok, dt, _} = DateTime.from_iso8601(time_str)
        dt

      %DateTime{} = dt ->
        # Already a DateTime, use as-is
        dt

      timestamp when is_integer(timestamp) ->
        # Unix timestamp, convert to DateTime
        DateTime.from_unix!(timestamp)

      val ->
        val
    end
  end

  defp build_row_map(row, columns) do
    columns
    |> Enum.with_index()
    |> Enum.reduce(%{}, fn {col, idx}, acc ->
      Map.put(acc, col, Enum.at(row, idx))
    end)
  end

  # Private helper functions

  defp build_identifier(%Trifle.Stats.Nocturnal.Key{} = key, driver) do
    identifier_map = identifier_for(key, driver)

    if driver.joined_identifier == :full do
      identifier_map
    else
      # Convert timestamp to DateTime if it's an integer
      case identifier_map do
        %{at: timestamp} when is_integer(timestamp) ->
          %{identifier_map | at: DateTime.from_unix!(timestamp)}

        _ ->
          identifier_map
      end
    end
  end

  defp inc_query(identifier, data, table_name) do
    {columns, values, conflict_columns} = build_query_parts(identifier)
    increment_data = Jason.encode!(data)

    # Build the nested jsonb_set calls exactly like Ruby implementation
    jsonb_increments =
      Enum.reduce(data, "to_jsonb(#{table_name}.data)", fn {k, v}, acc ->
        "jsonb_set(#{acc}, '{#{k}}', (COALESCE(#{table_name}.data->>'#{k}', '0')::numeric + #{v})::text::jsonb)"
      end)

    """
    INSERT INTO #{table_name} (#{columns}, data) VALUES (#{values}, '#{increment_data}')
    ON CONFLICT (#{conflict_columns}) DO UPDATE SET data = #{jsonb_increments};
    """
  end

  defp set_query(identifier, data, table_name) do
    {columns, values, conflict_columns} = build_query_parts(identifier)

    # Use complete replacement for set operations (not field-by-field like inc)
    data_json = Jason.encode!(data)

    """
    INSERT INTO #{table_name} (#{columns}, data) VALUES (#{values}, '#{data_json}')
    ON CONFLICT (#{conflict_columns}) DO UPDATE SET data = '#{data_json}'::jsonb;
    """
  end

  defp get_query(identifiers, table_name) do
    # Build OR conditions exactly like Ruby version
    conditions =
      identifiers
      |> Enum.map(&build_identifier_condition/1)
      |> Enum.join(" OR ")

    "SELECT * FROM #{table_name} WHERE #{conditions};"
  end

  defp ping_query(key_string, at, data, ping_table_name) do
    at_iso =
      case at do
        %DateTime{} = dt ->
          DateTime.to_iso8601(dt)

        timestamp when is_integer(timestamp) ->
          DateTime.from_unix!(timestamp) |> DateTime.to_iso8601()

        _ ->
          raise ArgumentError, "Invalid timestamp format"
      end

    """
    INSERT INTO #{ping_table_name} (key, at, data) VALUES ('#{key_string}', '#{at_iso}', '#{Jason.encode!(data)}')
    ON CONFLICT (key) DO UPDATE SET at = '#{at_iso}', data = '#{Jason.encode!(data)}'::jsonb;
    """
  end

  defp scan_query(key, ping_table_name) do
    "SELECT at, data FROM #{ping_table_name} WHERE key = '#{key}' ORDER BY at DESC LIMIT 1;"
  end

  defp build_query_parts(identifier) do
    columns = Map.keys(identifier) |> Enum.join(", ")

    values =
      identifier
      |> Map.values()
      |> Enum.map(&format_value/1)
      |> Enum.join(", ")

    conflict_columns = Map.keys(identifier) |> Enum.join(", ")

    {columns, values, conflict_columns}
  end

  defp build_identifier_condition(identifier) do
    # Build condition like Ruby version: k = v AND k = v...
    identifier
    |> Enum.map(fn {k, v} -> "#{k} = #{format_value(v)}" end)
    |> Enum.join(" AND ")
  end

  defp build_data_map(rows, columns, _identifiers, joined_identifier) do
    column_indices =
      columns
      |> Enum.with_index()
      |> Map.new(fn {col, idx} -> {col, idx} end)

    Enum.reduce(rows, %{}, fn row, acc ->
      identifier = extract_identifier_from_row(row, column_indices, joined_identifier)
      data_json = Enum.at(row, column_indices["data"])

      # Handle both string and already-decoded JSONB data
      data =
        case data_json do
          str when is_binary(str) ->
            case Jason.decode(str) do
              {:ok, decoded} -> decoded
              {:error, _} -> %{}
            end

          map when is_map(map) ->
            map

          _ ->
            %{}
        end

      Map.put(acc, identifier, data)
    end)
  end

  defp extract_identifier_from_row(row, column_indices, :full) do
    # Joined identifier mode
    %{key: Enum.at(row, column_indices["key"])}
  end

  defp extract_identifier_from_row(row, column_indices, :partial) do
    # Partial joined mode
    %{
      key: Enum.at(row, column_indices["key"]),
      at: Enum.at(row, column_indices["at"])
    }
  end

  defp extract_identifier_from_row(row, column_indices, nil) do
    # Separated identifier mode
    %{
      key: Enum.at(row, column_indices["key"]),
      granularity: Enum.at(row, column_indices["granularity"]),
      at: Enum.at(row, column_indices["at"])
    }
  end

  defp format_value(value) when is_binary(value), do: "'#{value}'"

  defp format_value(value) when is_integer(value) and value > 1_000_000,
    do: "'#{DateTime.from_unix!(value) |> DateTime.to_iso8601()}'"

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
