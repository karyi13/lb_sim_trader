# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Commands

### Full Pipeline
```bash
python main.py full
```
Runs complete pipeline: fetch -> analyze -> generate ladder data -> generate K-line data

### Individual Stages
```bash
# Fetch data (incremental by default)
python main.py fetch
python main.py fetch --full-refresh
python main.py fetch --start-date 20240101 --end-date 20240131

# Analyze for limit-ups
python main.py analyze

# Generate visualization data
python main.py ladder  # ladder_data.js
python main.py kline   # kline_data.js + kline_data_core.js
python main.py kline --all-stocks  # generate for all stocks, not just limit-ups
```

### Testing
```bash
python -m pytest tests/ -v
python -m pytest tests/ -v --cov=. --cov-report=html
```

### CLI Query Tool
```bash
python cli.py query <date>           # Query limit-ups for a date
python cli.py search <keyword>       # Search stock by code/name
python cli.py stats                  # Show statistics
python cli.py trend --days 7         # Show N-day trend
python cli.py export <date> <file>   # Export to CSV
python cli.py interactive            # Interactive mode
```

## Architecture Overview

This is an A-share stock analysis system that tracks "limit-up" stocks (stocks hitting daily maximum price increase) and analyzes consecutive limit-up trends. The system has three main layers:

1. **Data Fetching** - PyTDX (primary) + AkShare (fallback) sources
2. **Analysis** - Limit-up identification, consecutive days calculation, board type classification
3. **Visualization** - Generates JS files for web interface (K-line charts and ladder visualization)

### Data Flow Pipeline

```
fetch (DataFetcher) -> analyze (Analyzer) -> generate visualization
     |                      |                      |
     v                      v                      v
Parquet file          Ladder Parquet         ladder_data.js
(stock_daily_latest)  (limit_up_ladder)      kline_data.js
                                             kline_data_core.js
```

## Dependency Injection System

Services are managed through `depend/di_container.py`. Always access services via the container:

```python
from depend.di_container import container

data_fetcher = container.get('data_fetcher')        # CompositeDataFetcher
data_validator = container.get('data_validator')    # DataValidator
data_storage = container.get('data_storage')        # DataStorage
```

### Registered Services

| Service Key | Class | Purpose |
|-------------|-------|---------|
| `data_fetcher` | CompositeDataFetcher | Tries PyTDX first, falls back to AkShare |
| `pytdx_fetcher` | PyTDXDataFetcher | Primary data source (TDX protocol) |
| `akshare_fetcher` | AkShareDataFetcher | Fallback data source (HTTP-based) |
| `data_validator` | DataValidator | Validates data completeness and OHLC relationships |
| `data_storage` | DataStorage | Parquet/CSV/JSON save/load with automatic backup |

### Configuration (depend/config.py)

Key settings:
- `DEFAULT_START_DATE`: '20241220'
- `MAX_WORKERS`: 20 (concurrent fetch workers)
- `CONCEPT_FETCH_WORKERS`: 10 (parallel concept fetching)
- `MAX_RETRIES`: 3 (retry attempts)
- `REQUEST_TIMEOUT`: 10 seconds
- `DEFAULT_OUTPUT_FILE`: data/stock_daily_latest.parquet
- `DEFAULT_LADDER_FILE`: data/limit_up_ladder.parquet
- `DEFAULT_KLINE_JS_FILE`: data/kline_data.js

## Key Classes and Functions

### main.py

- `DataFetcher.run()`: Fetches daily OHLCV data, supports incremental updates
- `Analyzer.process()`: Identifies limit-ups, calculates consecutive days, classifies board types
- `generate_ladder_data_for_html()`: Creates ladder_data.js
- `generate_kline_data()`: Creates kline_data.js (calls generate_kline_data_core internally)
- `_disconnect_all_tdx_connections()**: Important call to prevent program hanging on exit (PyTDX heartbeat thread issue)

### depend/services.py

- `CompositeDataFetcher`: Combines PyTDX and AkShare with fallback
- `DataValidator`: Validates required columns, OHLC relationships, duplicates, nulls
- `DataStorage`: Auto-detects format (parquet/csv/json), includes backup functionality
- `get_thread_api()`: Thread-local PyTDX connection management

## Important Implementation Details

### Thread Safety

PyTDX connections use thread-local storage (`threading.local()`) to handle concurrent requests. Each worker thread gets its own API connection with `heartbeat=True`.

**Note**: The `heartbeat=True` parameter creates non-daemon threads that prevent Python from exiting. Always call `_disconnect_all_tdx_connections()` in `finally` blocks (already done in main.py).

### Vectorized Operations

Analysis uses pandas vectorized operations instead of loops:
- `groupby().shift()` for next-day calculations
- `groupby().cumcount()` for consecutive day tracking
- ~10x speedup compared to iterative approaches

### Limit-Up Price Calculation

```python
# main.py line 516
limit = int(prev_close * multiplier * 100 + 0.49999) / 100.0
```

Multipliers: ST stocks (1.05), ChiNext/STAR (1.20), Main board (1.10)

### Date Format

All dates use `YYYYMMDD` format internally (e.g., "20240101").

### Incremental Updates

Default fetch mode is incremental - only fetches new trading days. Use `--full-refresh` for complete data refresh.

### Weekend Handling

`get_default_end_date()` automatically skips weekends and respects market close time (15:00).

## Frontend Data Structure

### ladder_data.js
```javascript
window.LADDER_DATA = {
    "20240115": {
        "1": [{code, name, price, limitUpDays, conceptThemes, nextDayOpenChangePct}],
        "2": [...]  // 2-day consecutive limit-ups
    }
}
```

### kline_data.js
```javascript
window.KLINE_DATA_GLOBAL = {
    "000001.SZ": {
        name: "平安银行",
        dates: ["2024-01-01", ...],
        values: [[open, close, low, high], ...],
        volumes: [1000000, ...]
    }
}
```

### kline_data_core.js
Simplified version containing only the latest date's limit-up stocks (~1MB), used for fast initial load.

## File Locations

| File | Purpose |
|------|---------|
| `main.py` | Main CLI entry point, data pipeline orchestration |
| `cli.py` | Interactive query tool with Rich output formatting |
| `depend/di_container.py` | DI container managing service instances |
| `depend/config.py` | Global configuration dataclass |
| `depend/services.py` | Core service implementations |
| `depend/interfaces.py` | Abstract base classes defining service contracts |
| `function/stock_concepts.py` | Stock concept/industry metadata fetching |
| `utils/logging_utils.py` | Structured logging and performance monitoring (@log_performance decorator) |
