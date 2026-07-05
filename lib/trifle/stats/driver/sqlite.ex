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

  def new(
        connection,
        table_name \\ "trifle_stats",
        ping_table_name \\ nil,
        joined_identifier \\ :full,
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
        case Exqlite.query(
               connection,
               """
                 CREATE TABLE IF NOT EXISTS #{table_name} (
                   key TEXT PRIMARY KEY,
                   data TEXT NOT NULL DEFAULT '{}'
                 )
               """,
               []
             ) do
          :ok -> :ok
          {:ok, _result} -> :ok
          {:error, reason} -> raise "Failed to create main table: #{inspect(reason)}"
        end

      :partial ->
        # Partial joined mode - key + at composite primary key
        case Exqlite.query(
               connection,
               """
                 CREATE TABLE IF NOT EXISTS #{table_name} (
                   key TEXT NOT NULL,
                   at TEXT NOT NULL,
                   data TEXT NOT NULL DEFAULT '{}',
                   PRIMARY KEY (key, at)
                 )
               """,
               []
             ) do
          :ok -> :ok
          {:ok, _result} -> :ok
          {:error, reason} -> raise "Failed to create main table: #{inspect(reason)}"
        end

      nil ->
        # Separated identifier mode - multi-column primary key
        case Exqlite.query(
               connection,
               """
                 CREATE TABLE IF NOT EXISTS #{table_name} (
                   key TEXT NOT NULL,
                   granularity TEXT NOT NULL,
                   at TEXT NOT NULL,
                   data TEXT NOT NULL DEFAULT '{}',
                   PRIMARY KEY (key, granularity, at)
                 )
               """,
               []
             ) do
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

  defp system_data_for(%Trifle.Stats.Nocturnal.Key{} = key, count, tracking_key) do
    tracking_key = tracking_key || key.key
    Trifle.Stats.Packer.pack(%{count: count, keys: %{tracking_key => count}})
  end

  def setup_ping_table!(connection, ping_table_name) do
    case Exqlite.query(
           connection,
           """
             CREATE TABLE IF NOT EXISTS #{ping_table_name} (
               key TEXT PRIMARY KEY,
               at TEXT NOT NULL,
               data TEXT NOT NULL DEFAULT '{}'
             )
           """,
           []
         ) do
      :ok -> :ok
      {:ok, _result} -> :ok
      {:error, reason} -> raise "Failed to create ping table: #{inspect(reason)}"
    end
  end

  def inc(keys, values, driver, count \\ 1, tracking_key \\ nil) do
    data = Trifle.Stats.Packer.pack(values)

    # Use transaction like Ruby version for atomicity
    Exqlite.transaction(driver.connection, fn conn ->
      Enum.each(keys, fn %Trifle.Stats.Nocturnal.Key{} = key ->
        identifier = identifier_for(key, driver)
        # Batch data fields to avoid SQLite parser stack overflow
        batch_data_operations(identifier, data, driver.table_name, conn, :inc)

        track_system_data(conn, key, driver, count, tracking_key)
      end)
    end)
  end

  defp inc_query(identifier, data, table_name) do
    # Build SQL exactly like Ruby version with JSON functions
    columns = Map.keys(identifier)
    columns_sql = Enum.join(columns, ", ")
    placeholders = Enum.join(List.duplicate("?", length(columns)), ", ")

    # Build JSON increment operations like Ruby - use flattened keys directly
    expression =
      Enum.reduce(data, "data", fn {k, v}, acc ->
        path = json_path_for(k)

        "json_set(#{acc}, '#{path}', IFNULL(json_extract(data, '#{path}'), 0) + #{numeric_value(k, v)})"
      end)

    query = """
    INSERT INTO #{table_name} (#{columns_sql}, data) VALUES (#{placeholders}, json(?))
    ON CONFLICT (#{columns_sql}) DO UPDATE SET data = #{expression};
    """

    {query, query_params(identifier, columns) ++ [Jason.encode!(data)]}
  end

  def set(keys, values, driver, count \\ 1, tracking_key \\ nil) do
    data = Trifle.Stats.Packer.pack(values)

    # Use transaction like Ruby version for atomicity
    Exqlite.transaction(driver.connection, fn conn ->
      Enum.each(keys, fn %Trifle.Stats.Nocturnal.Key{} = key ->
        identifier = identifier_for(key, driver)
        # Batch data fields to avoid SQLite parser stack overflow
        batch_data_operations(identifier, data, driver.table_name, conn, :set)

        track_system_data(conn, key, driver, count, tracking_key)
      end)
    end)
  end

  # System tracking: run additional increment query with modified key and data
  defp track_system_data(conn, key, driver, count, tracking_key) do
    if driver.system_tracking do
      system_identifier = system_identifier_for(key, driver)
      system_data = system_data_for(key, count, tracking_key)
      batch_data_operations(system_identifier, system_data, driver.table_name, conn, :inc)
    end
  end

  defp set_query(identifier, data, table_name) do
    columns = Map.keys(identifier)
    columns_sql = Enum.join(columns, ", ")
    placeholders = Enum.join(List.duplicate("?", length(columns)), ", ")

    # Set each packed field individually (preserves fields not in the payload)
    {expression, value_params} =
      Enum.reduce(data, {"data", []}, fn {k, v}, {expression, value_params} ->
        {"json_set(#{expression}, '#{json_path_for(k)}', json(?))",
         value_params ++ [Jason.encode!(v)]}
      end)

    query = """
    INSERT INTO #{table_name} (#{columns_sql}, data) VALUES (#{placeholders}, json(?))
    ON CONFLICT (#{columns_sql}) DO UPDATE SET data = #{expression};
    """

    {query, query_params(identifier, columns) ++ [Jason.encode!(data)] ++ value_params}
  end

  def get(keys, driver) do
    # Convert keys to identifiers exactly like Ruby
    identifiers =
      Enum.map(keys, fn %Trifle.Stats.Nocturnal.Key{} = key ->
        identifier_for(key, driver)
      end)

    # Get data using Ruby-style get_all approach
    data = get_all(identifiers, driver)

    # Map back to result order using simple_identifier for consistent lookup
    results =
      Enum.map(keys, fn %Trifle.Stats.Nocturnal.Key{} = key ->
        simple_identifier = simple_identifier_for(key, driver)
        raw_data = Map.get(data, simple_identifier, %{})
        Trifle.Stats.Packer.unpack(raw_data)
      end)

    results
  end

  defp get_all(identifiers, driver) do
    # Build query exactly like Ruby version with OR conditions
    {query, params} = get_query(identifiers, driver.table_name)
    {:ok, result} = Exqlite.query(driver.connection, query, params)

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
              # key column
              key: Enum.at(row, 0),
              # granularity column
              granularity: Enum.at(row, 1),
              # at column
              at: parse_timestamp_from_sqlite(Enum.at(row, 2))
            }
        end

      # Use simple_identifier for consistent map key
      simple_identifier = simple_identifier_for(temp_key, driver)

      # Parse JSON data from last column (like Ruby)
      data_json = List.last(row)

      json_data =
        case Jason.decode(data_json) do
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

    data
    |> Enum.chunk_every(batch_size)
    |> Enum.each(fn batch ->
      batch_data = Map.new(batch)

      {query, params} =
        case operation do
          :inc -> inc_query(identifier, batch_data, table_name)
          :set -> set_query(identifier, batch_data, table_name)
        end

      Exqlite.query!(conn, query, params)
    end)
  end

  # Helper to parse timestamp from SQLite consistently
  defp parse_timestamp_from_sqlite(timestamp_value) do
    case timestamp_value do
      time_str when is_binary(time_str) ->
        parse_rfc3339_datetime!(time_str)

      timestamp when is_integer(timestamp) ->
        DateTime.from_unix!(timestamp)

      %DateTime{} = dt ->
        dt

      %NaiveDateTime{} = naive ->
        DateTime.from_naive!(naive, "Etc/UTC")

      val ->
        raise ArgumentError, "Invalid SQLite timestamp value: #{inspect(val)}"
    end
  end

  defp parse_rfc3339_datetime!(value) do
    case DateTime.from_iso8601(String.trim(value)) do
      {:ok, dt, _} ->
        dt

      {:error, reason} ->
        raise ArgumentError,
              "Invalid RFC3339 SQLite timestamp #{inspect(value)}: #{inspect(reason)}"
    end
  end

  defp get_query(identifiers, table_name) do
    # Build OR conditions exactly like Ruby version, with parameter placeholders
    {conditions, params} =
      Enum.reduce(identifiers, {[], []}, fn identifier, {conditions, params} ->
        {condition_parts, params} =
          Enum.reduce(identifier, {[], params}, fn field, {parts, params} ->
            {part, field_params} = build_field_condition(field)
            {parts ++ [part], params ++ field_params}
          end)

        {conditions ++ [Enum.join(condition_parts, " AND ")], params}
      end)

    {"SELECT * FROM #{table_name} WHERE #{Enum.join(conditions, " OR ")};", params}
  end

  defp build_field_condition({:at, value}) when not is_binary(value) do
    formatted = format_datetime_for_sqlite(value)
    with_microseconds = String.replace_suffix(formatted, "Z", ".000000Z")

    {"(at = ? OR at = ?)", [formatted, with_microseconds]}
  end

  defp build_field_condition({key, value}) do
    {"#{key} = ?", [query_param(value)]}
  end

  def ping(%Trifle.Stats.Nocturnal.Key{} = key, values, driver) do
    if driver.joined_identifier do
      # Return :ok like Ruby version (joined mode doesn't support ping/scan)
      :ok
    else
      # Pack data like Ruby version: { data: values, at: key.at }
      data = Trifle.Stats.Packer.pack(%{data: values, at: key.at})
      {query, params} = ping_query(key.key, key.at, data, driver.ping_table_name)

      # Use transaction like Ruby version
      case Exqlite.transaction(driver.connection, fn conn ->
             Exqlite.query!(conn, query, params)
           end) do
        {:ok, _} -> :ok
        result -> result
      end
    end
  end

  defp ping_query(key_string, at, data, ping_table_name) do
    at_formatted = format_datetime_for_sqlite(at)
    encoded = Jason.encode!(data)

    query = """
    INSERT INTO #{ping_table_name} (key, at, data) VALUES (?, ?, json(?))
    ON CONFLICT (key) DO UPDATE SET at = ?, data = json(?);
    """

    {query, [to_string(key_string), at_formatted, encoded, at_formatted, encoded]}
  end

  def scan(%Trifle.Stats.Nocturnal.Key{} = key, driver) do
    if driver.joined_identifier do
      # Return empty array like Ruby version
      []
    else
      {query, params} = scan_query(key.key, driver.ping_table_name)
      {:ok, result} = Exqlite.query(driver.connection, query, params)

      case result.rows do
        [[_key, at_string, data_json]] ->
          # Parse JSON data
          case Jason.decode(data_json) do
            {:ok, data} ->
              case parse_timestamp_from_sqlite(at_string) do
                %DateTime{} = at_datetime ->
                  # Return [timestamp, data] array like Ruby version
                  [at_datetime, Trifle.Stats.Packer.unpack(data)]

                _ ->
                  []
              end

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
    {"SELECT key, at, data FROM #{ping_table_name} WHERE key = ? ORDER BY at DESC LIMIT 1;",
     [to_string(key_string)]}
  end

  defp query_params(identifier, columns) do
    Enum.map(columns, fn column -> query_param(Map.fetch!(identifier, column)) end)
  end

  defp query_param(%DateTime{} = value), do: format_datetime_for_sqlite(value)

  defp query_param(%NaiveDateTime{} = value),
    do: value |> DateTime.from_naive!("Etc/UTC") |> format_datetime_for_sqlite()

  defp query_param(value) when is_integer(value) or is_float(value), do: value
  defp query_param(value), do: to_string(value)

  defp numeric_value(_key, value) when is_number(value), do: to_string(value)

  defp numeric_value(key, _value) do
    raise ArgumentError, "increment requires numeric value for key #{inspect(key)}"
  end

  defp json_path_for(key) do
    "$." <> String.replace(to_string(key), "'", "''")
  end

  defp format_datetime_for_sqlite(%DateTime{} = value) do
    value
    |> DateTime.shift_zone!("Etc/UTC")
    |> DateTime.truncate(:second)
    |> DateTime.to_iso8601()
  end

  defp format_datetime_for_sqlite(timestamp) when is_integer(timestamp) do
    timestamp
    |> DateTime.from_unix!()
    |> format_datetime_for_sqlite()
  end

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
