# CLAUDE.md - Trifle.Stats Development Guide

This file provides guidance to Claude Code (claude.ai/code) when working with the Trifle.Stats Elixir library.

## Quick Setup

### Dependencies and Database Setup
```bash
# Install dependencies
mix deps.get

# Start local databases
cd .devops/docker/local_db
docker-compose up -d

# Verify databases are running
docker-compose ps
```

### Running Tests
```bash
# Run all tests
mix test

# Run tests with coverage
mix test --cover

# Run specific driver tests
mix test test/trifle/stats/driver/
mix test test/trifle/stats/aggregator/
mix test test/trifle/stats/designator/
```

## Project Structure

### Core Modules
- `Trifle.Stats` - Main API module with public interface
- `Trifle.Stats.Configuration` - Configuration management
- `Trifle.Stats.Packer` - Data serialization for storage
- `Trifle.Stats.Nocturnal` - Time manipulation and timeline generation
- `Trifle.Stats.Series` - Data processing wrapper with fluent interface

### Operations
- `Trifle.Stats.Operations.Timeseries.Increment` - Incremental tracking
- `Trifle.Stats.Operations.Timeseries.Set` - Absolute value setting  
- `Trifle.Stats.Operations.Timeseries.Classify` - Value categorization
- `Trifle.Stats.Operations.Timeseries.Values` - Data retrieval
- `Trifle.Stats.Operations.Status.Beam` - Status ping
- `Trifle.Stats.Operations.Status.Scan` - Latest status retrieval

### Storage Drivers
- `Trifle.Stats.Driver.Mongo` - MongoDB with bulk operations
- `Trifle.Stats.Driver.Postgres` - PostgreSQL with JSONB
- `Trifle.Stats.Driver.Sqlite` - SQLite with JSON1 extension  
- `Trifle.Stats.Driver.Redis` - Redis hash-based storage

### Data Processing
- `Trifle.Stats.Aggregator.*` - avg, max, min, sum aggregation
- `Trifle.Stats.Designator.*` - custom, geometric, linear classification
- `Trifle.Stats.Formatter.*` - category, timeline data transformation

## API Usage Examples

### Basic Operations
```elixir
# Configure driver (MongoDB example)
{:ok, conn} = Mongo.start_link(url: "mongodb://localhost:27017/test")
driver = Trifle.Stats.Driver.Mongo.new(conn)
config = Trifle.Stats.Configuration.configure(driver)

# Track incremental stats
Trifle.Stats.track("page_views", DateTime.utc_now(), %{count: 1}, config)

# Set absolute values
Trifle.Stats.assert("users_online", DateTime.utc_now(), %{count: 150}, config)

# Classify values using designators
Trifle.Stats.assort("response_times", DateTime.utc_now(), %{duration: 250}, config)

# Retrieve data
series_data = Trifle.Stats.values("page_views", from_date, to_date, :day, config)
```

### Series Processing (Pipe-Friendly API)
```elixir
# Create series wrapper
series = Trifle.Stats.series(series_data)

# Terminal operations (return computed values)
total_count = series |> Trifle.Stats.Series.aggregate_sum("count")
average_response = series |> Trifle.Stats.Series.aggregate_avg("response_time")
max_value = series |> Trifle.Stats.Series.aggregate_max("value")

# Terminal formatting operations (return formatted data)
timeline_data = series |> Trifle.Stats.Series.format_timeline("count")
category_data = series |> Trifle.Stats.Series.format_category("status")

# Transformation operations (return new Series for chaining)
# Clean API with separate arguments - no more comma-separated strings!
processed_series = series
|> Trifle.Stats.Series.transform_average("sum", "count", "avg")
|> Trifle.Stats.Series.transform_ratio("success", "total", "success_rate")

# Complex processing pipelines with nested paths
result = series
|> Trifle.Stats.Series.transform_average("metrics.sum", "metrics.count", "metrics.avg")
|> Trifle.Stats.Series.transform_stddev("response_times", "deviation")
|> Trifle.Stats.Series.aggregate_max("deviation")
```

### Precision Handling
High-precision arithmetic using the Decimal library for accurate calculations:

```elixir
# Enable precision mode globally  
config :trifle_stats, precision: [
  enabled: true,
  scale: 10,        # decimal places
  rounding: :half_up
]

# All calculations automatically use precision when enabled
series_data = Trifle.Stats.values("financial_data", from_date, to_date, :day, config)
series = Trifle.Stats.series(series_data)

# Returns Decimal structs when precision enabled, floats otherwise
precise_average = series |> Trifle.Stats.Series.avg("amount")
precise_ratio = series |> Trifle.Stats.Series.ratio("profit,revenue")

# Direct precision utilities
Trifle.Stats.Precision.divide(1, 3)  # => Decimal with configured precision
Trifle.Stats.Precision.percentage(sample, total)  # => Exact percentage
```

### Driver Configuration
```elixir
# MongoDB
{:ok, mongo} = Mongo.start_link(url: "mongodb://localhost:27017/test")
mongo_driver = Trifle.Stats.Driver.Mongo.new(mongo)

# PostgreSQL  
{:ok, postgres} = Postgrex.start_link(
  hostname: "localhost", 
  port: 5432,
  username: "postgres", 
  password: "password",
  database: "trifle_dev"
)
postgres_driver = Trifle.Stats.Driver.Postgres.new(postgres)

# SQLite
{:ok, sqlite} = :esqlite3.open("/tmp/test.db")
sqlite_driver = Trifle.Stats.Driver.Sqlite.new(sqlite)

# Redis
{:ok, redis} = Redix.start_link(host: "localhost", port: 6379)
redis_driver = Trifle.Stats.Driver.Redis.new(redis)
```

## Database Setup Commands

### Start Local Databases
```bash
cd .devops/docker/local_db
docker-compose up -d

# Verify all services are healthy
docker-compose ps
```

### Create Database Schemas
```bash
# PostgreSQL - create tables
mix run -e "
{:ok, conn} = Postgrex.start_link(hostname: \"localhost\", username: \"postgres\", password: \"password\", database: \"trifle_dev\")
Trifle.Stats.Driver.Postgres.setup!(conn)
"

# MongoDB - create indexes  
mix run -e "
{:ok, conn} = Mongo.start_link(url: \"mongodb://localhost:27017/test\")
Trifle.Stats.Driver.Mongo.setup!(conn)
"
```

### Database Access
```bash
# PostgreSQL
docker-compose exec postgres psql -U postgres -d trifle_dev

# MongoDB
docker-compose exec mongo mongosh

# Redis
docker-compose exec redis redis-cli
```

## Testing Strategy

### Test Categories
- **Unit Tests**: Individual module functionality
- **Integration Tests**: Driver compatibility and data flow
- **Performance Tests**: Benchmarking across drivers
- **Edge Case Tests**: Error handling and validation

### Test Database Isolation
```bash
# Each test uses isolated collections/tables
# PostgreSQL: test_trifle_stats_{random_id}
# MongoDB: test_collection_{random_id}  
# Redis: test:prefix:{random_id}
# SQLite: test_stats_{random_id}.db
```

### Running Specific Test Suites
```bash
# Core functionality
mix test test/trifle/stats_test.exs

# All drivers
mix test test/trifle/stats/driver/

# All aggregators  
mix test test/trifle/stats/aggregator/

# Performance benchmarks
mix test test/performance/ --include performance
```

## Development Workflow

### Adding New Features
1. Write tests first (TDD approach)
2. Implement feature with simple, efficient code
3. Test across all drivers
4. Update documentation
5. Performance validation

### Code Quality Standards
- **Behaviors**: Use `@behaviour` for protocols (drivers, aggregators, etc.)
- **Pattern Matching**: Leverage Elixir's pattern matching
- **Error Handling**: Use custom error modules consistently
- **Documentation**: Clear docstrings with examples
- **Performance**: Simple implementations over complex optimizations

### Driver Development
When adding/modifying drivers:
1. Follow existing driver behavior protocol
2. Implement all required callbacks
3. Test both joined and separated identifier modes
4. Include setup!/teardown methods
5. Handle transactions where supported

## Architecture Notes

### Data Compatibility
- **Ruby Compatibility**: All drivers store data in same format as Ruby trifle-stats
- **Storage Format**: Hierarchical data flattened with dot notation
- **Time Handling**: Unix timestamps with timezone support
- **Value Types**: Automatic normalization via Packer

### Performance Considerations  
- **Bulk Operations**: Drivers use batch operations where possible
- **Connection Pooling**: Reuse connections across operations
- **Memory Management**: Immutable data structures, minimal copying
- **Simple Code**: Readable implementations over premature optimization

### Time Series Features
- **Multi-Granularity**: second, minute, hour, day, week, month, quarter, year
- **Time Zones**: Configurable with database support
- **Timeline Generation**: Flexible time boundary calculations
- **Granularity Filtering**: Configurable granularity per operation

## Troubleshooting

### Common Issues

**Compilation Errors:**
```bash
# Clean and recompile
mix clean
mix deps.clean --all
mix deps.get
mix compile
```

**Database Connection Issues:**
```bash
# Check if databases are running
docker-compose ps

# Restart databases
docker-compose restart

# Check logs
docker-compose logs postgres
docker-compose logs mongo
docker-compose logs redis
```

**Test Failures:**
```bash
# Run with detailed output
mix test --trace

# Run specific failing test
mix test test/path/to/failing_test.exs:line_number
```

### Performance Issues
```bash
# Profile memory usage
mix test --cover --export-coverage default
mix test.coverage

# Benchmark specific operations
mix run test/performance/driver_benchmarks.exs
```

## Dependencies

### Production Dependencies
- `mongodb_driver` - MongoDB client
- `postgrex` - PostgreSQL client  
- `exqlite` - SQLite client
- `redix` - Redis client
- `jason` - JSON encoding/decoding
- `tzdata` - Timezone database
- `decimal` - High-precision arithmetic library

### Development Dependencies
- `ex_doc` - Documentation generation
- `benchee` - Performance benchmarking (to be added)
- `credo` - Code quality analysis (to be added)

## Current Status

### Implemented Features (95% Complete)
- ✅ All core operations (track, assert, values, assort, beam, scan)
- ✅ All storage drivers (MongoDB, PostgreSQL, SQLite, Redis)
- ✅ All aggregators (avg, max, min, sum)
- ✅ All designators (custom, geometric, linear)
- ✅ All formatters (category, timeline)
- ✅ Series system with fluent interface
- ✅ Error handling and validation
- ✅ Data compatibility with Ruby version

### TODO (High Priority)
- [ ] Comprehensive test suite (95%+ coverage target)
- [ ] Performance benchmarking
- [ ] Enhanced configuration validation
- [ ] Documentation examples
- [ ] Edge case handling
