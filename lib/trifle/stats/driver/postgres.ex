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

  defp system_data_for(%Trifle.Stats.Nocturnal.Key{} = key, count, tracking_key) do
    tracking_key = tracking_key || key.key
    Trifle.Stats.Packer.pack(%{count: count, keys: %{tracking_key => count}})
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

  def inc(keys, values, driver, count \\ 1, tracking_key \\ nil) do
    data = Trifle.Stats.Packer.pack(values)

    Postgrex.transaction(driver.connection, fn conn ->
      Enum.each(keys, fn %Trifle.Stats.Nocturnal.Key{} = key ->
        # Use the raw identifier directly without modification
        identifier = identifier_for(key, driver)
        {query, params} = inc_query(identifier, data, driver.table_name)
        Postgrex.query!(conn, query, params)

        track_system_data(conn, key, driver, count, tracking_key)
      end)
    end)
  end

  def set(keys, values, driver, count \\ 1, tracking_key \\ nil) do
    data = Trifle.Stats.Packer.pack(values)

    Postgrex.transaction(driver.connection, fn conn ->
      Enum.each(keys, fn %Trifle.Stats.Nocturnal.Key{} = key ->
        # Use the raw identifier directly without modification
        identifier = identifier_for(key, driver)
        {query, params} = set_query(identifier, data, driver.table_name)
        Postgrex.query!(conn, query, params)

        track_system_data(conn, key, driver, count, tracking_key)
      end)
    end)
  end

  # System tracking: run additional increment query with modified key and data
  defp track_system_data(conn, key, driver, count, tracking_key) do
    if driver.system_tracking do
      system_identifier = system_identifier_for(key, driver)
      system_data = system_data_for(key, count, tracking_key)
      {system_query, system_params} = inc_query(system_identifier, system_data, driver.table_name)
      Postgrex.query!(conn, system_query, system_params)
    end
  end

  def get(keys, driver) do
    identifiers =
      Enum.map(keys, fn %Trifle.Stats.Nocturnal.Key{} = key ->
        identifier_for(key, driver)
      end)

    # Get all data from database with Ruby-style OR query
    data = get_all(identifiers, driver)

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
      {query, params} = ping_query(key.key, key.at, data, driver.ping_table_name)

      # Use transaction like Ruby version
      case Postgrex.transaction(driver.connection, fn conn ->
             Postgrex.query!(conn, query, params)
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
      {query, params} = scan_query(key.key, driver.ping_table_name)
      result = Postgrex.query!(driver.connection, query, params)

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

  defp get_all(identifiers, driver) do
    # Build query exactly like Ruby version with OR conditions
    {query, params} = get_query(identifiers, driver.table_name)
    result = Postgrex.query!(driver.connection, query, params)

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

  defp inc_query(identifier, data, table_name) do
    columns = Map.keys(identifier)
    columns_sql = Enum.join(columns, ", ")
    placeholders = columns |> Enum.with_index(1) |> Enum.map_join(", ", fn {_c, i} -> "$#{i}" end)

    # Build the nested jsonb_set calls exactly like Ruby implementation
    expression =
      Enum.reduce(data, "to_jsonb(#{table_name}.data)", fn {k, v}, acc ->
        path = escape_string(to_string(k))

        "jsonb_set(#{acc}, '{#{path}}', (COALESCE(#{table_name}.data->>'#{path}', '0')::numeric + #{numeric_value(k, v)})::text::jsonb)"
      end)

    query = """
    INSERT INTO #{table_name} (#{columns_sql}, data) VALUES (#{placeholders}, $#{length(columns) + 1})
    ON CONFLICT (#{columns_sql}) DO UPDATE SET data = #{expression};
    """

    {query, query_params(identifier, columns) ++ [data]}
  end

  defp set_query(identifier, data, table_name) do
    columns = Map.keys(identifier)
    columns_sql = Enum.join(columns, ", ")
    placeholders = columns |> Enum.with_index(1) |> Enum.map_join(", ", fn {_c, i} -> "$#{i}" end)

    base_params = query_params(identifier, columns) ++ [data]

    # Set each packed field individually (preserves fields not in the payload)
    {expression, params} =
      Enum.reduce(data, {"to_jsonb(#{table_name}.data)", base_params}, fn {k, v},
                                                                          {expression, params} ->
        params = params ++ [v]
        path = escape_string(to_string(k))
        {"jsonb_set(#{expression}, '{#{path}}', $#{length(params)}::jsonb)", params}
      end)

    query = """
    INSERT INTO #{table_name} (#{columns_sql}, data) VALUES (#{placeholders}, $#{length(columns) + 1})
    ON CONFLICT (#{columns_sql}) DO UPDATE SET data = #{expression};
    """

    {query, params}
  end

  defp get_query(identifiers, table_name) do
    # Build OR conditions exactly like Ruby version, with parameter placeholders
    {conditions, params} =
      Enum.reduce(identifiers, {[], []}, fn identifier, {conditions, params} ->
        {condition_parts, params} =
          Enum.reduce(identifier, {[], params}, fn {k, v}, {parts, params} ->
            params = params ++ [query_param(k, v)]
            {parts ++ ["#{k} = $#{length(params)}"], params}
          end)

        {conditions ++ [Enum.join(condition_parts, " AND ")], params}
      end)

    {"SELECT * FROM #{table_name} WHERE #{Enum.join(conditions, " OR ")};", params}
  end

  defp ping_query(key_string, at, data, ping_table_name) do
    query = """
    INSERT INTO #{ping_table_name} (key, at, data) VALUES ($1, $2, $3)
    ON CONFLICT (key) DO UPDATE SET at = $2, data = $3::jsonb;
    """

    {query, [to_string(key_string), query_param(:at, at), data]}
  end

  defp scan_query(key, ping_table_name) do
    {"SELECT at, data FROM #{ping_table_name} WHERE key = $1 ORDER BY at DESC LIMIT 1;",
     [to_string(key)]}
  end

  defp query_params(identifier, columns) do
    Enum.map(columns, fn column -> query_param(column, Map.fetch!(identifier, column)) end)
  end

  defp query_param(:at, %DateTime{} = value), do: value
  defp query_param(:at, value) when is_integer(value), do: DateTime.from_unix!(value)
  defp query_param(_column, value), do: value

  defp numeric_value(_key, value) when is_number(value), do: to_string(value)

  defp numeric_value(key, _value) do
    raise ArgumentError, "increment requires numeric value for key #{inspect(key)}"
  end

  defp escape_string(value), do: String.replace(value, "'", "''")

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
