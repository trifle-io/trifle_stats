# Trifle.Stats

[![Hex Version](https://img.shields.io/hexpm/v/trifle_stats.svg)](https://hex.pm/packages/trifle_stats)
[![Elixir CI](https://github.com/trifle-io/trifle_stats/workflows/Elixir%20CI/badge.svg?branch=main)](https://github.com/trifle-io/trifle_stats)

Time-series metrics for Elixir. Track anything — signups, revenue, job durations — using the database you already have. No InfluxDB. No TimescaleDB. Just one call and your existing PostgreSQL, MongoDB, Redis, MySQL, or SQLite.

Part of the [Trifle](https://trifle.io) ecosystem. Also available in [Ruby](https://github.com/trifle-io/trifle-stats) and [Go](https://github.com/trifle-io/trifle_stats_go).

## Why Trifle.Stats?

- **No new infrastructure** — Uses your existing database. No dedicated time-series DB to deploy, maintain, or pay for.
- **One call, many dimensions** — Track nested breakdowns (revenue by country by channel) in a single `track` call. Automatic rollup across configurable time granularities.
- **Library-first** — Start with the package. Add [Trifle App](https://trifle.io/product-app) dashboards, [Trifle CLI](https://github.com/trifle-io/trifle-cli) terminal access, or AI agent integration via MCP when you need them.

## Quick Start

### 1. Install

```elixir
def deps do
  [
    {:trifle_stats, "~> 1.0"}
  ]
end
```

### 2. Configure

```elixir
# config/config.exs
config :trifle_stats,
  driver: Trifle.Stats.Driver.Postgres,
  granularities: [:hour, :day, :week, :month]
```

### 3. Track

```elixir
Trifle.Stats.track("orders", DateTime.utc_now(), %{
  count: 1,
  revenue: 4990,
  revenue_by_country: %{us: 4990},
  revenue_by_channel: %{organic: 4990}
})
```

### 4. Query

```elixir
Trifle.Stats.values("orders", ~U[2026-02-10 00:00:00Z], DateTime.utc_now(), :day)
#=> %{
#     at: [~U[2026-02-10 00:00:00Z], ~U[2026-02-11 00:00:00Z], ...],
#     values: [%{"count" => 12, "revenue" => 59880, ...}, ...]
#   }
```

### 5. Process with Series

```elixir
Trifle.Stats.values("orders", from, to, :day)
|> Trifle.Stats.series()
|> Trifle.Stats.Series.transform_average("revenue", "count", "avg_order")
|> Trifle.Stats.Series.aggregate_sum("count")
```

## Drivers

| Driver | Backend | Best for |
|--------|---------|----------|
| **Postgres** | JSONB upsert | Most production apps |
| **MongoDB** | Document upsert | Document-oriented stacks |
| **Redis** | Hash increment | High-throughput counters |
| **MySQL** | JSON column | MySQL shops |
| **SQLite** | JSON1 extension | Single-node apps, dev/test |
| **Process** | In-memory (ETS) | Testing |

## Features

- **Multiple time granularities** — minute, hour, day, week, month, quarter, year
- **Nested value hierarchies** — Track dimensional breakdowns in a single call
- **Pipe-friendly Series API** — Chain aggregators, transponders, and formatters
- **Precision mode** — Decimal-based arithmetic for financial data
- **Data compatible** — Same storage format as the Ruby and Go implementations

## Documentation

Full guides, API reference, and examples at **[trifle.io/trifle-stats-ex](https://trifle.io/trifle-stats-ex)**

## Trifle Ecosystem

Trifle.Stats is the tracking layer. The ecosystem grows with you:

| Component | What it does |
|-----------|-------------|
| **[Trifle App](https://trifle.io/product-app)** | Dashboards, alerts, scheduled reports, AI-powered chat. Cloud or self-hosted. |
| **[Trifle CLI](https://github.com/trifle-io/trifle-cli)** | Query and push metrics from the terminal. MCP server mode for AI agents. |
| **[Trifle::Stats (Ruby)](https://github.com/trifle-io/trifle-stats)** | Ruby implementation with the same API and storage format. |
| **[Trifle Stats (Go)](https://github.com/trifle-io/trifle_stats_go)** | Go implementation with the same API and storage format. |

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/trifle-io/trifle_stats.

## License

The package is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
