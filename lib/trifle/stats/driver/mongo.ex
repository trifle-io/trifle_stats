defmodule Trifle.Stats.Driver.Mongo do
  @moduledoc """
  MongoDB driver for Trifle.Stats with Ruby gem compatibility.

  Supports both joined and separated identifier modes, TTL expiration,
  and configurable collection names for full Ruby trifle-stats compatibility.
  """

  defstruct connection: nil,
            collection_name: "trifle_stats",
            separator: "::",
            write_concern: 1,
            joined_identifier: :full,
            expire_after: nil,
            system_tracking: true,
            bulk_write: true

  @doc """
  Create a new MongoDB driver instance.

  ## Parameters
  - `connection`: MongoDB connection
  - `collection_name`: Collection name (default: "trifle_stats")
  - `separator`: Key separator for joined modes (default: "::")
  - `write_concern`: Write concern level (default: 1)
  - `joined_identifier`: Use joined ("full"/"partial") or separated (nil) identifiers (default: "full")
  - `expire_after`: TTL in seconds for automatic document expiration (default: nil)

  ## Examples
      # Basic usage
      {:ok, conn} = Mongo.start_link(url: "mongodb://localhost:27017/test")
      driver = Trifle.Stats.Driver.Mongo.new(conn)

      # With custom options (Ruby compatible)
      driver = Trifle.Stats.Driver.Mongo.new(conn, "analytics", "::", 1, :full, 86400)

      # Using configuration options
      config = Trifle.Stats.Configuration.configure(
        Trifle.Stats.Driver.Mongo.new(conn),
        driver_options: %{
          collection_name: "analytics_stats",
          joined_identifier: nil,
          expire_after: 86400
        }
      )
  """
  def new(
        connection,
        collection_name \\ "trifle_stats",
        separator \\ "::",
        write_concern \\ 1,
        joined_identifier \\ :full,
        expire_after \\ nil,
        system_tracking \\ true,
        bulk_write \\ true
      ) do
    identifier_mode = normalize_joined_identifier(joined_identifier)

    %Trifle.Stats.Driver.Mongo{
      connection: connection,
      collection_name: collection_name,
      separator: separator,
      write_concern: write_concern,
      joined_identifier: identifier_mode,
      expire_after: expire_after,
      system_tracking: system_tracking,
      bulk_write: bulk_write
    }
  end

  @doc """
  Create a new MongoDB driver from configuration (Ruby compatible).
  This applies driver_options from the configuration to override defaults.
  """
  def from_config(connection, %Trifle.Stats.Configuration{} = config) do
    # Extract driver options with Ruby-compatible defaults
    collection_name =
      Trifle.Stats.Configuration.driver_option(config, :collection_name, "trifle_stats")

    joined_identifier = Trifle.Stats.Configuration.driver_option(config, :joined_identifier, :full)
    expire_after = Trifle.Stats.Configuration.driver_option(config, :expire_after, nil)
    system_tracking = Trifle.Stats.Configuration.driver_option(config, :system_tracking, true)
    bulk_write = Trifle.Stats.Configuration.driver_option(config, :bulk_write, true)

    new(connection, collection_name, config.separator, 1, joined_identifier, expire_after, system_tracking, bulk_write)
  end

  @doc """
  Setup MongoDB collections and indexes (Ruby compatible).

  ## Parameters
  - `connection`: MongoDB connection
  - `collection_name`: Collection name (default: "trifle_stats")
  - `joined_identifier`: Index strategy - "full"/"partial" for joined, nil for separated
  - `expire_after`: TTL seconds for automatic expiration (default: nil)

  ## Examples
      # Basic setup (joined identifier mode)
      Trifle.Stats.Driver.Mongo.setup!(conn)

      # Ruby-compatible separated mode with TTL
      Trifle.Stats.Driver.Mongo.setup!(conn, "analytics", nil, 86400)
  """
  def setup!(
        connection,
        collection_name \\ "trifle_stats",
        joined_identifier \\ :full,
        expire_after \\ nil,
        system_tracking \\ true
      ) do
    identifier_mode = normalize_joined_identifier(joined_identifier)

    # Create the collection
    Mongo.create(connection, collection_name)

    # Create appropriate indexes based on identifier mode (Ruby behavior)
    indexes =
      case identifier_mode do
        :full ->
          # Joined identifier mode: single key field
          [%{"key" => %{"key" => 1}, "unique" => true}]

        :partial ->
          # Partial joined mode: key + at fields
          [%{"key" => %{"key" => 1, "at" => -1}, "unique" => true}]

        nil ->
          # Separated identifier mode: key, granularity, at fields
          [%{"key" => %{"key" => 1, "granularity" => 1, "at" => -1}, "unique" => true}]
      end

    # Add TTL index if expire_after is specified (Ruby behavior)
    indexes =
      if expire_after do
        indexes ++ [%{"key" => %{"expire_at" => 1}, "expireAfterSeconds" => 0}]
      else
        indexes
      end

    # Create all indexes
    Mongo.create_indexes(connection, collection_name, indexes)
    :ok
  rescue
    e -> {:error, e}
  end

  @doc """
  Setup from configuration (convenience method).
  """
  def setup_from_config!(connection, %Trifle.Stats.Configuration{} = config) do
    collection_name =
      Trifle.Stats.Configuration.driver_option(config, :collection_name, "trifle_stats")

    joined_identifier = Trifle.Stats.Configuration.driver_option(config, :joined_identifier, :full)
    expire_after = Trifle.Stats.Configuration.driver_option(config, :expire_after, nil)
    system_tracking = Trifle.Stats.Configuration.driver_option(config, :system_tracking, true)

    setup!(connection, collection_name, joined_identifier, expire_after, system_tracking)
  end

  defp system_identifier_for(%Trifle.Stats.Nocturnal.Key{} = key, driver) do
    system_key = %Trifle.Stats.Nocturnal.Key{
      key: "__system__key__",
      granularity: key.granularity,
      at: key.at
    }
    identifier_for(system_key, driver)
  end

  defp system_data_for(%Trifle.Stats.Nocturnal.Key{} = key, count \\ 1) do
    Trifle.Stats.Packer.pack(%{data: %{count: count, keys: %{key.key => count}}})
  end

  def inc(keys, values, driver, count \\ 1) do
    data = Trifle.Stats.Packer.pack(%{data: values})

    if driver.bulk_write do
      # Use bulk_write for all operations
      operations =
        Enum.flat_map(keys, fn %Trifle.Stats.Nocturnal.Key{} = key ->
          filter = identifier_for(key, driver) |> convert_keys_to_strings()
          expire_at = if driver.expire_after, do: DateTime.add(key.at, driver.expire_after, :second), else: nil

          main_op = %{
            update_many: %{
              filter: filter,
              update: build_update("$inc", data, expire_at),
              upsert: true
            }
          }

          if driver.system_tracking do
            system_filter = system_identifier_for(key, driver) |> convert_keys_to_strings()
            system_data = system_data_for(key, count)
            system_op = %{
              update_many: %{
                filter: system_filter,
                update: build_update("$inc", system_data, expire_at),
                upsert: true
              }
            }
            [main_op, system_op]
          else
            [main_op]
          end
        end)

      bulk_write(driver, operations)
    else
      # Use individual operations (default behavior)
      Enum.each(keys, fn %Trifle.Stats.Nocturnal.Key{} = key ->
        filter = identifier_for(key, driver) |> convert_keys_to_strings()
        expire_at = if driver.expire_after, do: DateTime.add(key.at, driver.expire_after, :second), else: nil
        update = build_update("$inc", data, expire_at)

        Mongo.update_many(driver.connection, driver.collection_name, filter, update, upsert: true)

        # System tracking: run additional increment with modified key and data
        if driver.system_tracking do
          system_filter = system_identifier_for(key, driver) |> convert_keys_to_strings()
          system_data = system_data_for(key, count)
          system_update = build_update("$inc", system_data, expire_at)

          Mongo.update_many(driver.connection, driver.collection_name, system_filter, system_update, upsert: true)
        end
      end)
    end
  end

  def set(keys, values, driver, count \\ 1) do
    # For set operations, we want complete replacement of data field only
    packed_data = Trifle.Stats.Packer.pack(values)

    if driver.bulk_write do
      # Use bulk_write for all operations
      operations =
        Enum.flat_map(keys, fn %Trifle.Stats.Nocturnal.Key{} = key ->
          filter = identifier_for(key, driver) |> convert_keys_to_strings()
          expire_at = if driver.expire_after, do: DateTime.add(key.at, driver.expire_after, :second), else: nil

          set_data = %{data: packed_data}
          main_op = %{
            update_many: %{
              filter: filter,
              update: build_update("$set", set_data, expire_at),
              upsert: true
            }
          }

          if driver.system_tracking do
            system_filter = system_identifier_for(key, driver) |> convert_keys_to_strings()
            system_data = system_data_for(key, count)
            system_op = %{
              update_many: %{
                filter: system_filter,
                update: build_update("$inc", system_data, expire_at),
                upsert: true
              }
            }
            [main_op, system_op]
          else
            [main_op]
          end
        end)

      bulk_write(driver, operations)
    else
      # Use individual operations (default behavior)
      Enum.each(keys, fn %Trifle.Stats.Nocturnal.Key{} = key ->
        filter = identifier_for(key, driver) |> convert_keys_to_strings()
        expire_at = if driver.expire_after, do: DateTime.add(key.at, driver.expire_after, :second), else: nil

        # Use complete replacement for set operations - replace data field completely
        update =
          if expire_at do
            %{"$set" => %{data: packed_data, expire_at: expire_at}}
          else
            %{"$set" => %{data: packed_data}}
          end

        Mongo.update_many(driver.connection, driver.collection_name, filter, update, upsert: true)

        # System tracking: run additional increment with modified key and data
        if driver.system_tracking do
          system_filter = system_identifier_for(key, driver) |> convert_keys_to_strings()
          system_data = system_data_for(key, count)
          system_update = build_update("$inc", system_data, expire_at)

          Mongo.update_many(driver.connection, driver.collection_name, system_filter, system_update, upsert: true)
        end
      end)
    end
  end

  def get(keys, driver) do
    # Convert keys to identifier format (like Ruby)
    identifiers =
      Enum.map(keys, fn %Trifle.Stats.Nocturnal.Key{} = key ->
        identifier = identifier_for(key, driver)
        # Convert atom keys to string keys for MongoDB query
        convert_keys_to_strings(identifier)
      end)

    # Use $or query like Ruby version instead of $in
    data =
      Mongo.find(
        driver.connection,
        driver.collection_name,
        %{"$or" => identifiers}
      )
      |> Enum.reduce(%{}, fn d, acc ->
        # Create a temporary key struct from the database document to use simple_identifier
        temp_key =
          case driver.joined_identifier do
            :full ->
              # For full joined mode, use the key as-is
              %Trifle.Stats.Nocturnal.Key{key: d["key"]}

            :partial ->
              # For partial joined mode, key + at
              %Trifle.Stats.Nocturnal.Key{
                key: d["key"],
                at: parse_timestamp_from_mongo(d["at"])
              }

            nil ->
              # For separated mode, build key from individual fields
              %Trifle.Stats.Nocturnal.Key{
                key: d["key"],
                granularity: d["granularity"],
                at: parse_timestamp_from_mongo(d["at"])
              }
          end

        # Use simple_identifier for consistent map key
        simple_identifier =
          simple_identifier_for(temp_key, driver)

        Map.put(acc, simple_identifier, d["data"])
      end)

    # Map back to result order using simple_identifier for consistent lookup
    Enum.map(keys, fn %Trifle.Stats.Nocturnal.Key{} = key ->
      simple_identifier = simple_identifier_for(key, driver)
      raw_data = Map.get(data, simple_identifier, %{})
      # Return data directly like Ruby (already unpacked from MongoDB)
      raw_data
    end)
  end

  def ping(%Trifle.Stats.Nocturnal.Key{} = key, values, driver) do
    if driver.joined_identifier do
      # Return empty array like Ruby version
      []
    else
      # Pack data like Ruby version: { data: values, at: key.at }
      packed_data = Trifle.Stats.Packer.pack(%{data: values, at: key.at})

      # Use complex filter like Ruby version with identifier.slice(:key)
      identifier =
        identifier_for(key, driver) |> convert_keys_to_strings()

      # Ruby's equivalent of slice(:key)
      filter = Map.take(identifier, ["key"])
      update = %{"$set" => packed_data}

      expire_at =
        if driver.expire_after, do: DateTime.add(key.at, driver.expire_after, :second), else: nil

      update =
        if expire_at do
          Map.put(update, "$set", Map.merge(update["$set"], %{expire_at: expire_at}))
        else
          update
        end

      # Use individual update operation for consistency
      Mongo.update_many(driver.connection, driver.collection_name, filter, update, upsert: true)
      :ok
    end
  end

  def scan(%Trifle.Stats.Nocturnal.Key{} = key, driver) do
    if driver.joined_identifier do
      # Return empty array like Ruby version
      []
    else
      # Find the document by key only and sort by 'at' descending like Ruby
      filter =
        Map.take(
          identifier_for(key, driver) |> convert_keys_to_strings(),
          ["key"]
        )

      options = [sort: %{at: -1}, limit: 1]

      case Mongo.find(driver.connection, driver.collection_name, filter, options)
           |> Enum.to_list() do
        [] ->
          # Return empty array like Ruby version
          []

        [doc] ->
          # Return [timestamp, data] array like Ruby version
          at =
            case doc["at"] do
              timestamp when is_number(timestamp) -> DateTime.from_unix!(timestamp)
              %DateTime{} = dt -> dt
              _ -> DateTime.utc_now()
            end

          # MongoDB automatically converts dotted keys to nested objects during storage
          # The original ping stored: Packer.pack(%{data: values, at: key.at})
          # Which MongoDB converted to: %{data: %{...nested values...}, at: timestamp}
          # So we unpack the structure excluding MongoDB metadata fields
          unpacked_data = Trifle.Stats.Packer.unpack(Map.drop(doc, ["_id", "key", "granularity"]))
          [at, unpacked_data]
      end
    end
  end

  # Helper to build update document with optional expire_at
  defp build_update(operation, data, expire_at) do
    if operation == "$set" && expire_at do
      %{operation => Map.put(data, :expire_at, expire_at)}
    else
      base = %{operation => data}
      if expire_at do
        Map.put(base, "$set", %{expire_at: expire_at})
      else
        base
      end
    end
  end

  # Helper to convert atom keys to string keys for MongoDB queries
  defp convert_keys_to_strings(map) when is_map(map) do
    Enum.reduce(map, %{}, fn {k, v}, acc ->
      string_key = if is_atom(k), do: Atom.to_string(k), else: k
      Map.put(acc, string_key, v)
    end)
  end

  # Helper to parse timestamp from MongoDB consistently
  defp parse_timestamp_from_mongo(timestamp_value) do
    case timestamp_value do
      %DateTime{} = dt ->
        # Already a DateTime, use as-is
        dt

      timestamp when is_integer(timestamp) ->
        # Unix timestamp, convert to DateTime
        DateTime.from_unix!(timestamp)

      time_str when is_binary(time_str) ->
        # Parse timestamp from string
        case DateTime.from_iso8601(time_str) do
          {:ok, dt, _} ->
            dt

          {:error, _} ->
            # Try parsing as integer unix timestamp
            case Integer.parse(time_str) do
              {timestamp, ""} -> DateTime.from_unix!(timestamp)
              _ -> time_str
            end
        end

      val ->
        val
    end
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

  defp bulk_write(_driver, []), do: :ok

  defp bulk_write(driver, operations) do
    bulk =
      Enum.reduce(operations, Mongo.UnorderedBulk.new(driver.collection_name), fn op, acc ->
        case op do
          %{update_many: %{filter: filter, update: update, upsert: upsert}} ->
            Mongo.UnorderedBulk.update_many(acc, filter, update, upsert: upsert)

          %{update_one: %{filter: filter, update: update, upsert: upsert}} ->
            Mongo.UnorderedBulk.update_one(acc, filter, update, upsert: upsert)

          _ ->
            acc
        end
      end)

    opts =
      case driver.write_concern do
        nil -> []
        concern -> [w: concern]
      end

    Mongo.BulkWrite.write(driver.connection, bulk, opts)
  end
end
