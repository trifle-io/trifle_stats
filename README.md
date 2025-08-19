# Trifle.Stats

[![Hex Version](https://img.shields.io/hexpm/v/trifle_stats.svg)](https://hex.pm/packages/trifle_stats)
[![Elixir CI](https://github.com/trifle-io/trifle_stats/workflows/Elixir%20CI/badge.svg?branch=main)](https://github.com/trifle-io/trifle_stats)

Simple analytics backed by MongoDB. It gets you from having bunch of events occuring within few minutes to being able to say what happened on 19th August 2023.

## Documentation

For comprehensive guides, API reference, and examples, visit [trifle.io/trifle-stats-ex](https://trifle.io/trifle-stats-ex)

## Installation

Add `trifle_stats` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:trifle_stats, "~> 1.0"}
  ]
end
```

Then run:

```bash
$ mix deps.get
```

## Quick Start

### 1. Configure

```elixir
# config/config.exs
config :trifle_stats,
  driver: Trifle.Stats.Driver.Mongo,
  granularities: [:minute, :hour, :day, :week, :month, :quarter, :year]

# Configure MongoDB connection
config :trifle_stats, Trifle.Stats.Driver.Mongo,
  hostname: "localhost",
  database: "trifle_stats",
  port: 27017
```

### 2. Track events

```elixir
Trifle.Stats.track("event::logs", DateTime.utc_now(), %{count: 1, duration: 2.11})
```

### 3. Retrieve values

```elixir
Trifle.Stats.values("event::logs", DateTime.utc_now |> DateTime.add(-30, :day), DateTime.utc_now(), :day)
#=> %{
#     at: [~U[2023-08-19 00:00:00Z]], 
#     values: [%{"count" => 1, "duration" => 2.11}]
#   }
```

## Features

- **Multiple time granularities** - Track data across different time periods
- **MongoDB backend** - Reliable document-based storage
- **Phoenix integration** - Easy integration with Phoenix applications
- **Performance optimized** - Efficient storage and retrieval patterns
- **Elixir native** - Built for the Elixir/OTP ecosystem

## Drivers

Currently supports:

- **MongoDB** - Document database with aggregation pipeline support

## Configuration

Configure your application in `config/config.exs`:

```elixir
config :trifle_stats,
  driver: Trifle.Stats.Driver.Mongo,
  granularities: [:minute, :hour, :day, :week, :month, :quarter, :year]
```

## Testing

Tests verify tracking functionality and data retrieval across time granularities. To run the test suite:

```bash
$ mix test
```

Ensure MongoDB is running locally for tests to pass.

Tests are meant to be **simple and isolated**. Every test should be **independent** and able to run in any order. Tests should be **self-contained** and set up their own configuration.

Use **single layer testing** to focus on testing a specific module or function in isolation. Use **appropriate mocking** for external dependencies when testing higher-level operations.

**Repeat yourself** in test setup for clarity rather than complex shared setups that can hide dependencies.

Tests verify that events are properly tracked, time granularities are correctly calculated, and data retrieval returns expected results. Database tests use test-specific collections to avoid conflicts.

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/trifle-io/trifle_stats.

## License

The package is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
