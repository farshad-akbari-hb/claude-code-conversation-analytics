# Claude Analytics Runbook

Operational procedures for the Claude Analytics Platform.

## Table of Contents

1. [Daily Operations](#daily-operations)
2. [Troubleshooting](#troubleshooting)
3. [Recovery Procedures](#recovery-procedures)
4. [Maintenance](#maintenance)
5. [Monitoring](#monitoring)

---

## Daily Operations

### Check Pipeline Status

```bash
# View Prefect dashboard
open http://localhost:4200

# Check deployment status
make status

# View worker logs
make logs

# Check recent flow runs
docker-compose -f docker-compose.analytics.yml exec analytics-worker \
  prefect flow-run ls --limit 10
```

### Verify Data Freshness

```sql
-- Run in Metabase or DuckDB CLI
SELECT
    MAX(extracted_at) as last_extraction,
    MAX(partition_date) as latest_date,
    COUNT(*) as total_records
FROM raw.conversations;
```

### Quick Health Check

```bash
# All services healthy?
docker-compose -f docker-compose.analytics.yml ps

# DuckDB has data?
docker-compose -f docker-compose.analytics.yml exec analytics-worker \
  python -c "
import duckdb
conn = duckdb.connect('/duckdb/analytics.db')
print('Raw records:', conn.execute('SELECT COUNT(*) FROM raw.conversations').fetchone()[0])
print('Sessions:', conn.execute('SELECT COUNT(*) FROM marts.dim_sessions').fetchone()[0])
"
```

---

## Troubleshooting

### Pipeline Failed

**Symptoms**: Prefect shows failed flow run

**Steps**:

1. Check flow run logs in Prefect UI (http://localhost:4200)

2. Identify which task failed:
   ```bash
   make logs | grep -i error
   ```

3. Common issues:

   | Error | Cause | Fix |
   |-------|-------|-----|
   | MongoDB connection refused | MongoDB not running | Start MongoDB |
   | DuckDB locked | Concurrent access | Kill other connections |
   | dbt compilation error | SQL syntax | Fix model, run `dbt compile` |
   | Iceberg write error | Disk full | Clear old snapshots |

4. Retry the flow:
   ```bash
   # Via Makefile
   make run-adhoc

   # Or direct CLI
   make pipeline
   ```

### No Data in Metabase

**Symptoms**: Dashboards show "No results"

**Steps**:

1. Check DuckDB has data:
   ```bash
   docker-compose -f docker-compose.analytics.yml exec analytics-worker \
     python -c "
   import duckdb
   conn = duckdb.connect('/duckdb/analytics.db')
   print(conn.execute('SELECT COUNT(*) FROM raw.conversations').fetchone())
   "
   ```

2. If no data, run extraction:
   ```bash
   docker-compose -f docker-compose.analytics.yml exec analytics-worker \
     python -m analytics.cli extract --full-backfill
   ```

3. Load to DuckDB:
   ```bash
   docker-compose -f docker-compose.analytics.yml exec analytics-worker \
     python -m analytics.cli load
   ```

4. Run dbt:
   ```bash
   docker-compose -f docker-compose.analytics.yml exec analytics-worker \
     cd /app/dbt && dbt run
   ```

### dbt Models Failing

**Symptoms**: `dbt run` returns errors

**Steps**:

1. Run dbt in debug mode:
   ```bash
   cd dbt && dbt run --debug
   ```

2. Test specific model:
   ```bash
   dbt run --select stg_conversations
   dbt test --select stg_conversations
   ```

3. Check source freshness:
   ```bash
   dbt source freshness
   ```

4. Common fixes:

   | Error | Fix |
   |-------|-----|
   | Source not found | Run loader to populate raw.conversations |
   | Column not found | Check extractor schema matches dbt source |
   | Type mismatch | Update dbt model casting |

### Metabase Connection Error

**Symptoms**: "Database connection failed"

**Steps**:

1. Verify DuckDB file exists:
   ```bash
   docker-compose -f docker-compose.analytics.yml exec metabase \
     ls -la /duckdb/
   ```

2. Check Metabase can read file:
   ```bash
   docker-compose -f docker-compose.analytics.yml exec metabase \
     cat /duckdb/analytics.db | head -c 100
   ```

3. Restart Metabase:
   ```bash
   docker-compose -f docker-compose.analytics.yml restart metabase
   ```

---

## Recovery Procedures

### Full Data Rebuild

When data is corrupted or needs complete refresh:

```bash
# Stop services
make down

# Remove DuckDB (DESTRUCTIVE)
docker volume rm analytics_duckdb-data

# Remove Iceberg data
docker volume rm analytics_analytics-data

# Restart infrastructure
make up-prefect

# Run full backfill
make run-backfill
```

### Restore from MongoDB

If DuckDB is lost but MongoDB is intact:

```bash
# Run full extraction
python -m analytics.cli extract --full-backfill

# Load from Iceberg
python -m analytics.cli load --full-refresh

# Rebuild dbt models
cd dbt && dbt run --full-refresh
```

### Reset Prefect State

If Prefect has stale state or work pool issues:

```bash
# Stop services
make down

# Remove Prefect database
docker volume rm analytics_prefect-db-data

# Restart infrastructure
make up-prefect

# Wait for Prefect server to be healthy
sleep 30

# Re-deploy flows (work pool is auto-created by prefect-worker)
make deploy

# Verify deployments
make status
```

### Worker Not Processing Jobs

If scheduled runs aren't executing:

```bash
# Check worker status
make logs

# Verify work pool exists in Prefect UI
open http://localhost:4200/work-pools

# Restart worker
docker-compose -f docker-compose.analytics.yml restart prefect-worker

# Re-deploy if needed
make deploy
```

---

## Maintenance

### Weekly Tasks

1. **Check disk usage**:
   ```bash
   docker system df
   ```

2. **Clean up old Iceberg snapshots**:
   ```bash
   python -m analytics.cli iceberg expire-snapshots --older-than 7d
   ```

3. **Vacuum DuckDB**:
   ```sql
   VACUUM;
   ANALYZE;
   ```

### Monthly Tasks

1. **Update dependencies**:
   ```bash
   pip install --upgrade -r requirements.txt
   cd dbt && dbt deps
   ```

2. **Rebuild Docker images**:
   ```bash
   docker-compose -f docker-compose.analytics.yml build --no-cache
   ```

3. **Review data quality trends**:
   - Check Great Expectations history
   - Review failed expectations patterns

---

## Monitoring

### Scheduled Deployments

The following pipelines run automatically:

| Deployment | Schedule | Description |
|------------|----------|-------------|
| `hourly-analytics` | Every hour | Incremental sync from MongoDB |
| `daily-full-refresh` | 2:00 AM | Full table refresh |

Manual deployments (trigger with `make run-*`):
- `adhoc-analytics` - On-demand incremental run
- `full-backfill` - Historical data reload

### Key Metrics to Watch

| Metric | Healthy Range | Action if Exceeded |
|--------|---------------|-------------------|
| Extraction time | < 5 min | Check MongoDB indexes |
| dbt run time | < 10 min | Review model complexity |
| DuckDB size | < 10 GB | Archive old data |
| Pipeline success rate | > 95% | Review failure patterns |

### Alerting

Configure alerts in Prefect for:

- Flow run failures
- Runs taking > 2x expected time
- No successful runs in 4 hours

### Log Locations

| Component | Location |
|-----------|----------|
| Prefect Worker | `make logs` or `docker logs prefect-worker` |
| Analytics Worker | `docker logs analytics-worker` |
| Prefect Server | `docker logs prefect-server` |
| Metabase | `docker logs metabase` |
| dbt | `dbt/logs/` |

---

## Contact

For issues not covered here:
1. Check GitHub Issues
2. Review docs/SPEC.md for architecture details
3. Check dbt documentation at http://localhost:8080
