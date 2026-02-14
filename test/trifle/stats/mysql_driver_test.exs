defmodule Trifle.Stats.MysqlDriverTest do
  use ExUnit.Case

  @moduletag :integration
  @moduletag :mysql

  # Helper function to convert array keys to Nocturnal::Key objects
  defp to_key([key_name, granularity, timestamp]) when is_binary(timestamp) do
    Trifle.Stats.Nocturnal.Key.new(
      key: key_name,
      granularity: granularity,
      at: DateTime.from_unix!(String.to_integer(timestamp))
    )
  end

  defp to_key([key_name, granularity, timestamp]) when is_integer(timestamp) do
    Trifle.Stats.Nocturnal.Key.new(
      key: key_name,
      granularity: granularity,
      at: DateTime.from_unix!(timestamp)
    )
  end

  defp to_keys(array_keys), do: Enum.map(array_keys, &to_key/1)

  describe "MySQL driver - configuration and mode handling" do
    test "new and description normalize joined identifier values" do
      full_driver = Trifle.Stats.Driver.Mysql.new(self(), "stats_full", :full)
      partial_driver = Trifle.Stats.Driver.Mysql.new(self(), "stats_partial", "partial")
      separated_driver = Trifle.Stats.Driver.Mysql.new(self(), "stats_separated", nil)

      assert full_driver.joined_identifier == :full
      assert partial_driver.joined_identifier == :partial
      assert separated_driver.joined_identifier == nil

      assert Trifle.Stats.Driver.Mysql.description(full_driver) ==
               "Elixir.Trifle.Stats.Driver.Mysql(J)"

      assert Trifle.Stats.Driver.Mysql.description(partial_driver) ==
               "Elixir.Trifle.Stats.Driver.Mysql(P)"

      assert Trifle.Stats.Driver.Mysql.description(separated_driver) ==
               "Elixir.Trifle.Stats.Driver.Mysql(S)"
    end

    test "new raises for invalid joined identifier values" do
      assert_raise ArgumentError, fn ->
        Trifle.Stats.Driver.Mysql.new(self(), "stats_invalid", :unexpected_mode)
      end
    end

    test "from_config applies mysql driver options" do
      config =
        Trifle.Stats.Configuration.configure(nil,
          validate_driver: false,
          driver_options: %{
            table_name: "analytics_stats",
            ping_table_name: "analytics_stats_ping",
            joined_identifier: :partial,
            system_tracking: false
          }
        )

      driver = Trifle.Stats.Driver.Mysql.from_config(self(), config)

      assert driver.table_name == "analytics_stats"
      assert driver.ping_table_name == "analytics_stats_ping"
      assert driver.joined_identifier == :partial
      assert driver.system_tracking == false
    end

    test "ping and scan are no-op in joined modes" do
      driver = Trifle.Stats.Driver.Mysql.new(self(), "stats_joined", :full)
      key = Trifle.Stats.Nocturnal.Key.new(key: "service_status", at: DateTime.utc_now())

      assert :ok = Trifle.Stats.Driver.Mysql.ping(key, %{status: "ok"}, driver)
      assert [] = Trifle.Stats.Driver.Mysql.scan(key, driver)
    end
  end

  describe "MySQL driver - joined identifier mode" do
    test "basic operations work" do
      if not mysql_available?() do
        IO.puts("Skipping MySQL tests - MySQL not available")
      else
        {:ok, conn} = start_mysql_connection()
        table_name = "test_mysql_#{:rand.uniform(1_000_000)}"

        driver = Trifle.Stats.Driver.Mysql.new(conn, table_name, :full)
        assert :ok = Trifle.Stats.Driver.Mysql.setup!(conn, table_name, :full)

        keys =
          to_keys([["page_views", "hour", 1_692_266_400], ["page_views", "day", 1_692_230_400]])

        Trifle.Stats.Driver.Mysql.inc(keys, %{count: 5, amount: 100}, driver)
        results = Trifle.Stats.Driver.Mysql.get(keys, driver)

        assert Enum.all?(results, fn result ->
                 result["count"] == 5 and result["amount"] == 100
               end)

        Trifle.Stats.Driver.Mysql.inc(keys, %{count: 3, amount: 50}, driver)
        updated = Trifle.Stats.Driver.Mysql.get(keys, driver)

        assert Enum.all?(updated, fn result ->
                 result["count"] == 8 and result["amount"] == 150
               end)

        Trifle.Stats.Driver.Mysql.set(keys, %{count: 20, status: "active"}, driver)
        set_results = Trifle.Stats.Driver.Mysql.get(keys, driver)

        assert Enum.all?(set_results, fn result ->
                 result["count"] == 20 and result["status"] == "active" and
                   result["amount"] == nil
               end)

        assert [%{}] =
                 Trifle.Stats.Driver.Mysql.get(
                   to_keys([["non_existent", "hour", 1_692_266_400]]),
                   driver
                 )

        MyXQL.query!(conn, "DROP TABLE IF EXISTS #{table_name}", [])
        GenServer.stop(conn)
      end
    end

    test "system tracking uses count and custom tracking key" do
      if not mysql_available?() do
        IO.puts("Skipping MySQL tests - MySQL not available")
      else
        {:ok, conn} = start_mysql_connection()
        table_name = "test_mysql_system_#{:rand.uniform(1_000_000)}"

        driver = Trifle.Stats.Driver.Mysql.new(conn, table_name, :full, nil, true)
        assert :ok = Trifle.Stats.Driver.Mysql.setup!(conn, table_name, :full)

        key = to_key(["event_logs", "hour", 1_692_266_400])
        Trifle.Stats.Driver.Mysql.inc([key], %{count: 2}, driver, "manual")

        system_key =
          Trifle.Stats.Nocturnal.Key.new(
            key: "__system__key__",
            granularity: "hour",
            at: key.at
          )

        [system_values] = Trifle.Stats.Driver.Mysql.get([system_key], driver)
        assert system_values["count"] == 1
        assert system_values["keys"]["manual"] == 1

        MyXQL.query!(conn, "DROP TABLE IF EXISTS #{table_name}", [])
        GenServer.stop(conn)
      end
    end

    test "partial mode stores key and timestamp separately" do
      if not mysql_available?() do
        IO.puts("Skipping MySQL tests - MySQL not available")
      else
        {:ok, conn} = start_mysql_connection()
        table_name = "test_mysql_partial_#{:rand.uniform(1_000_000)}"

        driver = Trifle.Stats.Driver.Mysql.new(conn, table_name, :partial)
        assert :ok = Trifle.Stats.Driver.Mysql.setup!(conn, table_name, :partial)

        key = to_key(["page_views", "hour", 1_692_266_400])
        Trifle.Stats.Driver.Mysql.inc([key], %{count: 1}, driver)

        result =
          MyXQL.query!(
            conn,
            "SELECT `key`, `at` FROM `#{table_name}` WHERE `key` = ? LIMIT 1",
            ["page_views::hour"]
          )

        assert [[stored_key, _stored_at]] = result.rows
        assert stored_key == "page_views::hour"

        [values] = Trifle.Stats.Driver.Mysql.get([key], driver)
        assert values["count"] == 1

        MyXQL.query!(conn, "DROP TABLE IF EXISTS #{table_name}", [])
        GenServer.stop(conn)
      end
    end
  end

  describe "MySQL driver - separated identifier mode" do
    test "ping and scan operations work" do
      if not mysql_available?() do
        IO.puts("Skipping MySQL tests - MySQL not available")
      else
        {:ok, conn} = start_mysql_connection()
        table_name = "test_mysql_sep_#{:rand.uniform(1_000_000)}"
        ping_table_name = "#{table_name}_ping"

        driver = Trifle.Stats.Driver.Mysql.new(conn, table_name, nil, ping_table_name)
        assert :ok = Trifle.Stats.Driver.Mysql.setup!(conn, table_name, nil, ping_table_name)

        at = DateTime.utc_now()
        key = Trifle.Stats.Nocturnal.Key.new(key: "service_status", at: at)
        assert :ok = Trifle.Stats.Driver.Mysql.ping(key, %{status: "running", count: 25}, driver)

        scan_key = Trifle.Stats.Nocturnal.Key.new(key: "service_status")
        [scan_at, scan_values] = Trifle.Stats.Driver.Mysql.scan(scan_key, driver)

        assert %DateTime{} = scan_at
        assert scan_values["data"]["status"] == "running"
        assert scan_values["data"]["count"] == 25

        new_key =
          Trifle.Stats.Nocturnal.Key.new(key: "service_status", at: DateTime.add(at, 60, :second))

        assert :ok = Trifle.Stats.Driver.Mysql.ping(new_key, %{status: "idle", count: 10}, driver)

        [_latest_at, latest_values] = Trifle.Stats.Driver.Mysql.scan(scan_key, driver)
        assert latest_values["data"]["status"] == "idle"
        assert latest_values["data"]["count"] == 10

        assert [] =
                 Trifle.Stats.Driver.Mysql.scan(
                   Trifle.Stats.Nocturnal.Key.new(key: "missing"),
                   driver
                 )

        MyXQL.query!(conn, "DROP TABLE IF EXISTS #{table_name}", [])
        MyXQL.query!(conn, "DROP TABLE IF EXISTS #{ping_table_name}", [])
        GenServer.stop(conn)
      end
    end

    test "set and get work with separated identifiers" do
      if not mysql_available?() do
        IO.puts("Skipping MySQL tests - MySQL not available")
      else
        {:ok, conn} = start_mysql_connection()
        table_name = "test_mysql_sep_values_#{:rand.uniform(1_000_000)}"

        driver = Trifle.Stats.Driver.Mysql.new(conn, table_name, nil)
        assert :ok = Trifle.Stats.Driver.Mysql.setup!(conn, table_name, nil)

        keys = to_keys([["orders", "hour", 1_692_266_400], ["orders", "day", 1_692_230_400]])
        Trifle.Stats.Driver.Mysql.set(keys, %{clicks: 10, views: 50}, driver)
        Trifle.Stats.Driver.Mysql.inc([Enum.at(keys, 0)], %{clicks: 5}, driver)

        [first, second] = Trifle.Stats.Driver.Mysql.get(keys, driver)
        assert first["clicks"] == 15
        assert first["views"] == 50
        assert second["clicks"] == 10
        assert second["views"] == 50

        MyXQL.query!(conn, "DROP TABLE IF EXISTS #{table_name}", [])
        MyXQL.query!(conn, "DROP TABLE IF EXISTS #{table_name}_ping", [])
        GenServer.stop(conn)
      end
    end
  end

  defp start_mysql_connection do
    MyXQL.start_link(
      hostname: System.get_env("MYSQL_HOST", "localhost"),
      port: System.get_env("MYSQL_PORT", "3306") |> String.to_integer(),
      username: System.get_env("MYSQL_USER", "root"),
      password: System.get_env("MYSQL_PASSWORD", "password"),
      database: System.get_env("MYSQL_DATABASE", "trifle_dev"),
      connect_timeout: 1_000,
      timeout: 1_000
    )
  end

  defp mysql_available? do
    host = System.get_env("MYSQL_HOST", "localhost")
    port = System.get_env("MYSQL_PORT", "3306") |> String.to_integer()

    case :gen_tcp.connect(String.to_charlist(host), port, [:binary, active: false], 1_000) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
        true

      {:error, _reason} ->
        false
    end
  end
end
