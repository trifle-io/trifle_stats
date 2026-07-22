defmodule Trifle.Stats.ApiDriverTest do
  use ExUnit.Case

  alias Trifle.Stats.Configuration
  alias Trifle.Stats.Driver.Api

  test "track and assert bypass buffering and send gzip JSON immediately" do
    parent = self()

    transport = fn request ->
      send(parent, {:request, request})
      {:ok, {{~c"HTTP/1.1", 201, ~c"Created"}, [], ""}}
    end

    driver = Api.new("secret", "project-1", transport: transport)
    config = Configuration.configure(driver, buffer_enabled: true)
    at = ~U[2026-07-22 12:00:00Z]

    assert :ok = Trifle.Stats.track("orders", at, %{count: 1}, config, untracked: true)
    assert :ok = Trifle.Stats.assert("state::orders", at, %{pending: 4}, config)
    assert config.storage == driver

    assert_received {:request, track_request}
    assert_received {:request, assert_request}
    assert Jason.decode!(:zlib.gunzip(track_request.body))["operation"] == "track"
    assert Jason.decode!(:zlib.gunzip(assert_request.body))["operation"] == "assert"
    assert {~c"content-encoding", ~c"gzip"} in track_request.headers
    assert {~c"authorization", ~c"Bearer secret"} in track_request.headers
    assert {~c"x-trifle-source-id", ~c"project-1"} in track_request.headers
    assert track_request.url == ~c"https://app.trifle.io/api/v1/metrics"
  end

  test "surfaces overload metadata without retrying" do
    parent = self()

    transport = fn request ->
      send(parent, {:request, request})
      {:ok, {{~c"HTTP/1.1", 429, ~c"Too Many Requests"}, [{~c"retry-after", ~c"3"}], "busy"}}
    end

    driver = Api.new("secret", "project-1", transport: transport)

    assert {:error, %Api.Error{status: 429, retry_after: "3"}} =
             Api.direct_write(:track, "orders", DateTime.utc_now(), %{count: 1}, driver, [])

    assert_received {:request, _request}
    refute_received {:request, _request}
  end

  test "validates credentials and rejects unsupported operations" do
    assert_raise ArgumentError, fn -> Api.new("", "project-1") end
    driver = Api.new("secret", "project-1")
    assert {:error, {:unsupported_operation, :values, :api_driver}} = Api.get([], driver)
  end

  test "marks transport failures as unknown delivery without retrying" do
    parent = self()

    transport = fn request ->
      send(parent, {:request, request})
      {:error, :timeout}
    end

    driver = Api.new("secret", "project-1", transport: transport)

    assert {:error, %Api.Error{delivery_unknown: true}} =
             Api.direct_write(:track, "orders", DateTime.utc_now(), %{count: 1}, driver, [])

    assert_received {:request, _request}
    refute_received {:request, _request}
  end
end
