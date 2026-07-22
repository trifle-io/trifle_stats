defmodule Trifle.Stats.Driver.Api do
  @moduledoc """
  Synchronous write-only driver for Trifle Cloud projects.

  Track and assert calls bypass the local buffer and are delivered immediately.
  The driver does not retry because a timed-out increment may already have been committed.
  """

  @endpoint ~c"https://app.trifle.io/api/v1/metrics"
  @timeout 10_000
  @error_body_limit 1024

  defstruct connection: :api, token: nil, project_id: nil, transport: nil

  defmodule Error do
    defexception [:message, :status, :response_body, :retry_after, delivery_unknown: false]
  end

  defmodule Httpc do
    @moduledoc false

    def request(request) do
      ssl_options = [
        verify: :verify_peer,
        cacerts: :public_key.cacerts_get(),
        customize_hostname_check: [
          match_fun: :public_key.pkix_verify_hostname_match_fun(:https)
        ]
      ]

      :httpc.request(
        :post,
        {request.url, request.headers, ~c"application/json", request.body},
        [connect_timeout: request.timeout, timeout: request.timeout, ssl: ssl_options],
        body_format: :binary
      )
    end
  end

  def new(token, project_id, opts \\ []) do
    token = to_string(token)
    project_id = to_string(project_id)

    if String.trim(token) == "", do: raise(ArgumentError, "token must not be empty")
    if String.trim(project_id) == "", do: raise(ArgumentError, "project_id must not be empty")

    %__MODULE__{
      token: token,
      project_id: project_id,
      transport: Keyword.get(opts, :transport, Httpc)
    }
  end

  def bypass_buffer?(_driver), do: true

  def direct_write(operation, key, at, values, driver, opts)
      when operation in [:track, :assert] do
    payload = %{
      operation: Atom.to_string(operation),
      key: key,
      at: DateTime.to_iso8601(at),
      values: values,
      untracked: Keyword.get(opts, :untracked, false)
    }

    request = %{
      url: @endpoint,
      headers: [
        {~c"authorization", to_charlist("Bearer " <> driver.token)},
        {~c"x-trifle-source-id", to_charlist(driver.project_id)},
        {~c"accept", ~c"application/json"},
        {~c"content-encoding", ~c"gzip"},
        {~c"user-agent", to_charlist(user_agent())}
      ],
      body: payload |> Jason.encode!() |> :zlib.gzip(),
      timeout: @timeout
    }

    case transport_request(driver.transport, request) do
      {:ok, {{_http_version, status, _reason}, _headers, _body}} when status in 200..299 ->
        :ok

      {:ok, {{_http_version, status, _reason}, headers, body}} ->
        {:error,
         %Error{
           message: "Trifle API returned HTTP #{status}",
           status: status,
           response_body: body |> to_string() |> binary_part_safe(@error_body_limit),
           retry_after: header(headers, ~c"retry-after")
         }}

      {:error, reason} ->
        {:error,
         %Error{
           message: "Trifle API request failed: #{inspect(reason)}",
           delivery_unknown: true
         }}
    end
  end

  def inc(_keys, _values, _driver, _count \\ 1, _tracking_key \\ nil),
    do: unsupported(:track)

  def set(_keys, _values, _driver, _count \\ 1, _tracking_key \\ nil),
    do: unsupported(:assert)

  def get(_keys, _driver), do: unsupported(:values)
  def ping(_key, _values, _driver), do: unsupported(:beam)
  def scan(_key, _driver), do: unsupported(:scan)

  defp transport_request(transport, request) when is_function(transport, 1),
    do: transport.(request)

  defp transport_request(transport, request), do: transport.request(request)

  defp header(headers, name) do
    Enum.find_value(headers, fn {key, value} ->
      if String.downcase(to_string(key)) == to_string(name), do: to_string(value)
    end)
  end

  defp binary_part_safe(value, limit) when byte_size(value) <= limit, do: value
  defp binary_part_safe(value, limit), do: binary_part(value, 0, limit)

  defp user_agent do
    version = Application.spec(:trifle_stats, :vsn) || ~c"unknown"
    "trifle-stats-elixir/#{version}"
  end

  defp unsupported(operation),
    do: {:error, {:unsupported_operation, operation, :api_driver}}
end
