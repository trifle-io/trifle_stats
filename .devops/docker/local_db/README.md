# Local Database Setup for Trifle.Stats

This docker-compose configuration provides all database services needed for Trifle.Stats development and testing, allowing you to test all storage drivers locally.

## Services

- **PostgreSQL**: Port 5432 (with JSONB support)
- **MongoDB**: Port 27017 (with bulk operations)
- **Redis**: Port 6379 (hash-based storage)
- **SQLite**: File-based (no service needed, but test container available)

## Usage

1. Start the databases:
   ```bash
   cd .devops/docker/local_db
   docker-compose up -d
   ```

2. Verify all services are healthy:
   ```bash
   docker-compose ps
   ```

3. Run your Elixir tests:
   ```bash
   # From project root
   mix deps.get
   mix test
   ```

4. Stop the databases:
   ```bash
   docker-compose down
   ```

## Configuration

The databases are configured for testing all Trifle.Stats drivers:

### PostgreSQL
- **Host**: `localhost:5432`
- **User**: `postgres`
- **Password**: `password` 
- **Database**: `trifle_dev`
- **Features**: JSONB support, UPSERT operations, transactions

### MongoDB
- **Host**: `localhost:27017`
- **Features**: Bulk operations, TTL indexes, upserts
- **Collections**: Auto-created with proper indexes

### Redis
- **Host**: `localhost:6379`
- **Features**: Hash operations (HINCRBY, HMSET, HGETALL)
- **Prefix**: Configurable key prefixing

### SQLite
- **Location**: File-based in your project directory
- **Features**: JSON1 extension, UPSERT support
- **Files**: `stats.db`, `stats_test.db` (auto-created)

## Testing All Drivers

Run comprehensive tests against all database backends:

```bash
# Test all drivers
mix test --include integration

# Test specific driver
mix test test/trifle/stats/driver/postgres_test.exs
mix test test/trifle/stats/driver/mongo_test.exs
mix test test/trifle/stats/driver/redis_test.exs
mix test test/trifle/stats/driver/sqlite_test.exs
```

## Data Persistence

Data is persisted in Docker volumes:
- `postgres_data` - PostgreSQL tables and indexes
- `mongo_data` - MongoDB collections and documents
- `redis_data` - Redis hashes and keys
- `sqlite_data` - SQLite database files (also accessible locally)

## Development Commands

### Database Operations
```bash
# Reset all databases
docker-compose down -v
docker-compose up -d

# Access individual databases
docker-compose exec postgres psql -U postgres -d trifle_dev
docker-compose exec mongo mongosh
docker-compose exec redis redis-cli

# View logs
docker-compose logs postgres
docker-compose logs mongo  
docker-compose logs redis
```

### Setup Database Schemas
```bash
# PostgreSQL - create tables and indexes
mix run -e "Trifle.Stats.Driver.Postgres.setup!(connection, \"trifle_stats\")"

# MongoDB - create collections and indexes  
mix run -e "Trifle.Stats.Driver.Mongo.setup!(connection, \"trifle_stats\")"

# SQLite - create tables (auto-created on first use)
# Redis - no schema needed
```

## Performance Testing

Test performance across all drivers:

```bash
# Run performance benchmarks
mix run test/performance/benchmarks.exs

# Load test with concurrent operations
mix run test/performance/load_test.exs
```

## Health Checks

All services include health checks. Monitor with:

```bash
# Check service health
docker-compose ps

# Detailed health status
docker inspect trifle-stats-local-db-postgres-1 --format='{{.State.Health.Status}}'
docker inspect trifle-stats-local-db-mongo-1 --format='{{.State.Health.Status}}'
docker inspect trifle-stats-local-db-redis-1 --format='{{.State.Health.Status}}'
```

## Connection Examples

### Elixir Connection Setup

```elixir
# config/dev.exs or config/test.exs

# PostgreSQL
config :postgrex,
  hostname: "localhost",
  port: 5432,
  username: "postgres", 
  password: "password",
  database: "trifle_dev"

# MongoDB  
config :mongodb_driver,
  url: "mongodb://localhost:27017/trifle_stats"

# Redis
config :redix,
  host: "localhost",
  port: 6379

# SQLite (file-based, no network config needed)
```

## Troubleshooting

### Port Conflicts
If ports are already in use, modify the docker-compose.yml:
```yaml
# Change ports (host:container)
ports:
  - "5433:5432"  # PostgreSQL
  - "27018:27017"  # MongoDB  
  - "6380:6379"  # Redis
```

### Permission Issues
```bash
# Fix volume permissions
docker-compose down
docker volume rm trifle-stats-local-db_postgres_data
docker volume rm trifle-stats-local-db_mongo_data
docker-compose up -d
```

### Service Not Starting
```bash
# Check specific service logs
docker-compose logs postgres
docker-compose logs mongo
docker-compose logs redis

# Restart specific service
docker-compose restart postgres
```
