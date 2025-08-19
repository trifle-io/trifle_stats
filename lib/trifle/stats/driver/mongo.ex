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
            joined_identifier: true,
            expire_after: nil

  @doc """
  Create a new MongoDB driver instance.

  ## Parameters
  - `connection`: MongoDB connection
  - `collection_name`: Collection name (default: "trifle_stats")
  - `separator`: Key separator for joined mode (default: "::")
  - `write_concern`: Write concern level (default: 1)
  - `joined_identifier`: Use joined (true) or separated (false) identifiers (default: true)
  - `expire_after`: TTL in seconds for automatic document expiration (default: nil)

  ## Examples
      # Basic usage
      {:ok, conn} = Mongo.start_link(url: "mongodb://localhost:27017/test")
      driver = Trifle.Stats.Driver.Mongo.new(conn)
      
      # With custom options (Ruby compatible)
      driver = Trifle.Stats.Driver.Mongo.new(conn, "analytics", "::", 1, true, 86400)
      
      # Using configuration options
      config = Trifle.Stats.Configuration.configure(
        Trifle.Stats.Driver.Mongo.new(conn),
        driver_options: %{
          collection_name: "analytics_stats",
          joined_identifier: false,
          expire_after: 86400
        }
      )
  """
  def new(
        connection,
        collection_name \\ "trifle_stats",
        separator \\ "::",
        write_concern \\ 1,
        joined_identifier \\ true,
        expire_after \\ nil
      ) do
    %Trifle.Stats.Driver.Mongo{
      connection: connection,
      collection_name: collection_name,
      separator: separator,
      write_concern: write_concern,
      joined_identifier: joined_identifier,
      expire_after: expire_after
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

    joined_identifier = Trifle.Stats.Configuration.driver_option(config, :joined_identifier, true)
    expire_after = Trifle.Stats.Configuration.driver_option(config, :expire_after, nil)

    new(connection, collection_name, config.separator, 1, joined_identifier, expire_after)
  end

  @doc """
  Setup MongoDB collections and indexes (Ruby compatible).

  ## Parameters
  - `connection`: MongoDB connection
  - `collection_name`: Collection name (default: "trifle_stats")
  - `joined_identifier`: Index strategy - true for joined, false for separated
  - `expire_after`: TTL seconds for automatic expiration (default: nil)

  ## Examples
      # Basic setup (joined identifier mode)
      Trifle.Stats.Driver.Mongo.setup!(conn)
      
      # Ruby-compatible separated mode with TTL
      Trifle.Stats.Driver.Mongo.setup!(conn, "analytics", false, 86400)
  """
  def setup!(
        connection,
        collection_name \\ "trifle_stats",
        joined_identifier \\ true,
        expire_after \\ nil
      ) do
    # Create the collection
    Mongo.create(connection, collection_name)

    # Create appropriate indexes based on identifier mode (Ruby behavior)
    indexes =
      if joined_identifier do
        # Joined identifier mode: single key field
        [%{"key" => %{"key" => 1}, "unique" => true}]
      else
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

    joined_identifier = Trifle.Stats.Configuration.driver_option(config, :joined_identifier, true)
    expire_after = Trifle.Stats.Configuration.driver_option(config, :expire_after, nil)

    setup!(connection, collection_name, joined_identifier, expire_after)
  end

  def inc(keys, values, driver) do
    data = Trifle.Stats.Packer.pack(%{data: values})

    # Use individual operations instead of bulk_write for MongoDB compatibility
    Enum.each(keys, fn %Trifle.Stats.Nocturnal.Key{} = key ->
      filter =
        Trifle.Stats.Nocturnal.Key.identifier(key, driver.separator) |> convert_keys_to_strings()

      expire_at =
        if driver.expire_after, do: DateTime.add(key.at, driver.expire_after, :second), else: nil

      # Use individual update_many operations
      update =
        if expire_at do
          %{"$inc" => data, "$set" => %{expire_at: expire_at}}
        else
          %{"$inc" => data}
        end

      Mongo.update_many(driver.connection, driver.collection_name, filter, update, upsert: true)
    end)
  end

  def set(keys, values, driver) do
    # For set operations, we want complete replacement of data field only
    packed_data = Trifle.Stats.Packer.pack(values)

    # Use individual operations instead of bulk_write for MongoDB compatibility
    Enum.each(keys, fn %Trifle.Stats.Nocturnal.Key{} = key ->
      filter =
        Trifle.Stats.Nocturnal.Key.identifier(key, driver.separator) |> convert_keys_to_strings()

      expire_at =
        if driver.expire_after, do: DateTime.add(key.at, driver.expire_after, :second), else: nil

      # Use complete replacement for set operations - replace data field completely
      update =
        if expire_at do
          %{"$set" => %{data: packed_data, expire_at: expire_at}}
        else
          %{"$set" => %{data: packed_data}}
        end

      Mongo.update_many(driver.connection, driver.collection_name, filter, update, upsert: true)
    end)
  end

  def get(keys, driver) do
    # Convert keys to identifier format (like Ruby)
    identifiers =
      Enum.map(keys, fn %Trifle.Stats.Nocturnal.Key{} = key ->
        identifier = Trifle.Stats.Nocturnal.Key.identifier(key, driver.separator)
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
          if driver.joined_identifier do
            # For joined mode, parse the combined key back to components
            %Trifle.Stats.Nocturnal.Key{key: d["key"]}
          else
            # For separated mode, build key from individual fields
            %Trifle.Stats.Nocturnal.Key{
              key: d["key"],
              granularity: d["granularity"],
              at: parse_timestamp_from_mongo(d["at"])
            }
          end

        # Use simple_identifier for consistent map key
        simple_identifier =
          Trifle.Stats.Nocturnal.Key.simple_identifier(temp_key, driver.separator)

        Map.put(acc, simple_identifier, d["data"])
      end)

    # Map back to result order using simple_identifier for consistent lookup
    Enum.map(keys, fn %Trifle.Stats.Nocturnal.Key{} = key ->
      simple_identifier = Trifle.Stats.Nocturnal.Key.simple_identifier(key, driver.separator)
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
        Trifle.Stats.Nocturnal.Key.identifier(key, driver.separator) |> convert_keys_to_strings()

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
          Trifle.Stats.Nocturnal.Key.identifier(key, driver.separator)
          |> convert_keys_to_strings(),
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
end
