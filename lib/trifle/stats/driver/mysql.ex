defmodule Trifle.Stats.Driver.Mysql do
  @moduledoc """
  MySQL driver for Trifle.Stats using JSON columns for efficient JSON storage.
  Supports joined and separated identifier modes with atomic upserts.
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

  def from_config(connection, %Trifle.Stats.Configuration{} = config) do
    table_name = Trifle.Stats.Configuration.driver_option(config, :table_name, "trifle_stats")
    ping_table_name = Trifle.Stats.Configuration.driver_option(config, :ping_table_name, nil)

    joined_identifier =
      Trifle.Stats.Configuration.driver_option(config, :joined_identifier, :full)

    system_tracking = Trifle.Stats.Configuration.driver_option(config, :system_tracking, true)

    new(connection, table_name, joined_identifier, ping_table_name, system_tracking)
  end

  def setup!(
        connection,
        table_name \\ "trifle_stats",
        joined_identifier \\ :full,
        ping_table_name \\ nil,
        _system_tracking \\ true
      ) do
    ping_table = ping_table_name || "#{table_name}_ping"
    identifier_mode = normalize_joined_identifier(joined_identifier)
    quoted_table = quote_identifier(table_name)
    quoted_ping_table = quote_identifier(ping_table)

    case identifier_mode do
      :full ->
        MyXQL.query!(
          connection,
          """
            CREATE TABLE IF NOT EXISTS #{quoted_table} (
              `key` VARCHAR(255) PRIMARY KEY,
              `data` JSON NOT NULL
            )
          """,
          []
        )

      :partial ->
        MyXQL.query!(
          connection,
          """
            CREATE TABLE IF NOT EXISTS #{quoted_table} (
              `key` VARCHAR(255) NOT NULL,
              `at` DATETIME(6) NOT NULL,
              `data` JSON NOT NULL,
              PRIMARY KEY (`key`, `at`)
            )
          """,
          []
        )

      nil ->
        MyXQL.query!(
          connection,
          """
            CREATE TABLE IF NOT EXISTS #{quoted_table} (
              `key` VARCHAR(255) NOT NULL,
              `granularity` VARCHAR(255) NOT NULL,
              `at` DATETIME(6) NOT NULL,
              `data` JSON NOT NULL,
              PRIMARY KEY (`key`, `granularity`, `at`)
            )
          """,
          []
        )

        MyXQL.query!(
          connection,
          """
            CREATE TABLE IF NOT EXISTS #{quoted_ping_table} (
              `key` VARCHAR(255) PRIMARY KEY,
              `at` DATETIME(6) NOT NULL,
              `data` JSON NOT NULL
            )
          """,
          []
        )
    end

    :ok
  end

  def description(driver) do
    mode =
      if driver.joined_identifier == :full do
        "J"
      else
        if driver.joined_identifier == :partial, do: "P", else: "S"
      end

    "#{__MODULE__}(#{mode})"
  end

  def inc(keys, values, driver, count \\ 1, tracking_key \\ nil) do
    data = Trifle.Stats.Packer.pack(values)

    MyXQL.transaction(driver.connection, fn conn ->
      Enum.each(keys, fn %Trifle.Stats.Nocturnal.Key{} = key ->
        identifier = identifier_for(key, driver)
        {query, params} = inc_query(identifier, data, driver.table_name)
        MyXQL.query!(conn, query, params)

        track_system_data(conn, key, driver, count, tracking_key)
      end)
    end)
  end

  def set(keys, values, driver, count \\ 1, tracking_key \\ nil) do
    data = Trifle.Stats.Packer.pack(values)

    MyXQL.transaction(driver.connection, fn conn ->
      Enum.each(keys, fn %Trifle.Stats.Nocturnal.Key{} = key ->
        identifier = identifier_for(key, driver)
        {query, params} = set_query(identifier, data, driver.table_name)
        MyXQL.query!(conn, query, params)

        track_system_data(conn, key, driver, count, tracking_key)
      end)
    end)
  end

  def get(keys, driver) do
    identifiers =
      Enum.map(keys, fn %Trifle.Stats.Nocturnal.Key{} = key -> identifier_for(key, driver) end)

    data = get_all(identifiers, driver)

    Enum.map(identifiers, fn identifier ->
      data
      |> Map.get(lookup_key_for(identifier), %{})
      |> Trifle.Stats.Packer.unpack()
    end)
  end

  def ping(%Trifle.Stats.Nocturnal.Key{} = key, values, driver) do
    if driver.joined_identifier do
      :ok
    else
      packed = Trifle.Stats.Packer.pack(%{data: values, at: key.at})
      {query, params} = ping_query(key.key, key.at, packed, driver.ping_table_name)

      case MyXQL.transaction(driver.connection, fn conn ->
             MyXQL.query!(conn, query, params)
           end) do
        {:ok, _} -> :ok
        result -> result
      end
    end
  end

  def scan(%Trifle.Stats.Nocturnal.Key{} = key, driver) do
    if driver.joined_identifier do
      []
    else
      query =
        "SELECT `at`, CAST(`data` AS CHAR) AS data FROM #{quote_identifier(driver.ping_table_name)} WHERE `key` = ? ORDER BY `at` DESC LIMIT 1"

      result = MyXQL.query!(driver.connection, query, [key.key])

      case result.rows do
        [[at_value, data_payload]] ->
          with {:ok, at_datetime} <- to_datetime(at_value),
               {:ok, decoded} <- decode_json_payload(data_payload) do
            [at_datetime, Trifle.Stats.Packer.unpack(decoded)]
          else
            _ -> []
          end

        _ ->
          []
      end
    end
  end

  # Private helper functions

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

  defp track_system_data(conn, key, driver, count, tracking_key) do
    if driver.system_tracking do
      system_identifier = system_identifier_for(key, driver)
      system_data = system_data_for(key, count, tracking_key)

      {system_query, system_params} = inc_query(system_identifier, system_data, driver.table_name)

      MyXQL.query!(conn, system_query, system_params)
    end
  end

  # Fetch all identifiers with a single query (mirrors Ruby's get_all).
  # The `at` column is selected as a plain string to avoid client time zone casting.
  defp get_all(identifiers, driver) do
    columns = identifiers |> hd() |> Map.keys()

    columns_sql =
      Enum.map_join(columns, ", ", fn
        :at -> "DATE_FORMAT(`at`, '%Y-%m-%d %H:%i:%s.%f') AS at"
        column -> quote_identifier(column)
      end)

    conditions =
      Enum.map_join(identifiers, " OR ", fn _identifier ->
        inner =
          Enum.map_join(columns, " AND ", fn column -> "#{quote_identifier(column)} = ?" end)

        "(#{inner})"
      end)

    query =
      "SELECT #{columns_sql}, CAST(`data` AS CHAR) AS data FROM #{quote_identifier(driver.table_name)} WHERE #{conditions}"

    args =
      Enum.flat_map(identifiers, fn identifier ->
        Enum.map(columns, &normalize_query_value(Map.fetch!(identifier, &1)))
      end)

    result = MyXQL.query!(driver.connection, query, args)

    Enum.reduce(result.rows, %{}, fn row, acc ->
      identifier = columns |> Enum.zip(row) |> Map.new()

      case decode_json_payload(List.last(row)) do
        {:ok, decoded} -> Map.put(acc, lookup_key_for(identifier), decoded)
        {:error, _reason} -> acc
      end
    end)
  end

  defp lookup_key_for(identifier) do
    identifier
    |> Enum.sort_by(fn {column, _value} -> to_string(column) end)
    |> Enum.map_join("|", fn {_column, value} -> lookup_value(value) end)
  end

  defp lookup_value(%DateTime{} = value), do: format_time_value(value)

  defp lookup_value(%NaiveDateTime{} = value),
    do: value |> DateTime.from_naive!("Etc/UTC") |> format_time_value()

  defp lookup_value(value), do: to_string(value)

  defp format_time_value(%DateTime{} = value) do
    utc = DateTime.shift_zone!(value, "Etc/UTC")
    {microseconds, _precision} = utc.microsecond

    Calendar.strftime(utc, "%Y-%m-%d %H:%M:%S") <>
      "." <> String.pad_leading(Integer.to_string(microseconds), 6, "0")
  end

  defp inc_query(identifier, data, table_name) do
    columns = Map.keys(identifier)
    identifier_values = Enum.map(columns, &normalize_query_value(Map.fetch!(identifier, &1)))
    packed_entries = Enum.sort_by(data, fn {k, _v} -> to_string(k) end)

    column_sql = columns |> Enum.map(&quote_identifier/1) |> Enum.join(", ")
    value_sql = Enum.join(List.duplicate("?", length(columns)) ++ ["CAST(? AS JSON)"], ", ")
    conflict_sql = build_inc_json_set_expression(packed_entries)

    query = """
    INSERT INTO #{quote_identifier(table_name)} (#{column_sql}, `data`) VALUES (#{value_sql})
    ON DUPLICATE KEY UPDATE `data` = #{conflict_sql};
    """

    params = identifier_values ++ [Jason.encode!(data)] ++ increment_values(packed_entries)
    {query, params}
  end

  defp set_query(identifier, data, table_name) do
    columns = Map.keys(identifier)
    identifier_values = Enum.map(columns, &normalize_query_value(Map.fetch!(identifier, &1)))
    packed_entries = Enum.sort_by(data, fn {k, _v} -> to_string(k) end)

    column_sql = columns |> Enum.map(&quote_identifier/1) |> Enum.join(", ")
    value_sql = Enum.join(List.duplicate("?", length(columns)) ++ ["CAST(? AS JSON)"], ", ")
    conflict_sql = build_set_json_set_expression(packed_entries)

    query = """
    INSERT INTO #{quote_identifier(table_name)} (#{column_sql}, `data`) VALUES (#{value_sql})
    ON DUPLICATE KEY UPDATE `data` = #{conflict_sql};
    """

    params =
      identifier_values ++ [Jason.encode!(data)] ++ serialized_set_values(packed_entries)

    {query, params}
  end

  defp ping_query(key, at, data, ping_table_name) do
    query = """
    INSERT INTO #{quote_identifier(ping_table_name)} (`key`, `at`, `data`) VALUES (?, ?, CAST(? AS JSON))
    ON DUPLICATE KEY UPDATE `at` = VALUES(`at`), `data` = VALUES(`data`);
    """

    params = [key, normalize_query_value(at), Jason.encode!(data)]
    {query, params}
  end

  defp build_inc_json_set_expression(entries) do
    Enum.reduce(entries, "JSON_SET(COALESCE(`data`, JSON_OBJECT())", fn {key, _value}, acc ->
      path = json_path_for(key)

      "#{acc}, '#{path}', (COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(COALESCE(`data`, JSON_OBJECT()), '#{path}')) AS DECIMAL(65,10)), 0) + CAST(? AS DECIMAL(65,10)))"
    end) <> ")"
  end

  defp build_set_json_set_expression(entries) do
    Enum.reduce(entries, "JSON_SET(COALESCE(`data`, JSON_OBJECT())", fn {key, _value}, acc ->
      "#{acc}, '#{json_path_for(key)}', CAST(? AS JSON)"
    end) <> ")"
  end

  defp serialized_set_values(entries) do
    Enum.map(entries, fn {_key, value} -> Jason.encode!(value) end)
  end

  defp increment_values(entries) do
    Enum.map(entries, fn {key, value} ->
      if is_number(value) do
        value
      else
        raise ArgumentError, "increment requires numeric value for key #{inspect(key)}"
      end
    end)
  end

  defp decode_json_payload(payload) when is_binary(payload), do: Jason.decode(payload)
  defp decode_json_payload(payload) when is_map(payload), do: {:ok, payload}
  defp decode_json_payload(_payload), do: {:error, :invalid_payload}

  defp normalize_query_value(%DateTime{} = value),
    do: value |> DateTime.shift_zone!("Etc/UTC") |> DateTime.to_naive()
  defp normalize_query_value(%NaiveDateTime{} = value), do: value
  defp normalize_query_value(value), do: value

  defp to_datetime(%DateTime{} = value), do: {:ok, value}
  defp to_datetime(%NaiveDateTime{} = value), do: DateTime.from_naive(value, "Etc/UTC")
  defp to_datetime(value) when is_integer(value), do: {:ok, DateTime.from_unix!(value)}

  defp to_datetime(value) when is_binary(value) do
    iso =
      if String.contains?(value, "T") do
        value
      else
        String.replace(value, " ", "T")
      end

    with {:ok, naive} <- NaiveDateTime.from_iso8601(iso) do
      DateTime.from_naive(naive, "Etc/UTC")
    end
  end

  defp to_datetime(_value), do: {:error, :invalid_datetime}

  defp json_path_for(key) do
    key
    |> to_string()
    |> String.replace("\\", "\\\\")
    |> String.replace("\"", "\\\"")
    |> String.replace("'", "''")
    |> then(&"$.\"#{&1}\"")
  end

  defp quote_identifier(identifier) do
    escaped = identifier |> to_string() |> String.replace("`", "``")
    "`#{escaped}`"
  end

  defp identifier_for(%Trifle.Stats.Nocturnal.Key{} = key, driver) do
    Trifle.Stats.Nocturnal.Key.identifier(key, driver.separator, driver.joined_identifier)
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
