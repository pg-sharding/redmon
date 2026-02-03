# REDMON - SPQR Redistribution Monitor

Automated monitoring and key range redistribution across SPQR shards.

## Features

- Monitor task group status
- Auto-retry failed tasks (max 4 per iteration)
- Redistribute key ranges between shards
- Run long-lived commands in tmux sessions
- Log to file and console simultaneously
- Dry-run mode for testing

## Quick Start

### Dry-run mode:

```bash
python3 spqr_monitor.py --dry-run
```

### Production mode:

```bash
python3 spqr_monitor.py --log-file ~/logs/spqr_monitor.log
```

## Options

```
--db-host HOST              Database host (default: localhost)
--db-port PORT              Database port (default: 6432)
--db-name NAME              Database name (default: spqr-console)
--db-user USER              Database user (default: spqr-console)
--iteration-timeout SECONDS Interval between iterations (default: 60)
--log-file PATH             Log file path (default: ./spqr_monitor.log)
--dry-run                   Test mode - print commands only
```

## How it works

Each iteration:

1. Check if database is read-only
2. Retry task groups with ERROR status (max 4)
3. Check if all task groups are RUNNING (>= 8)
4. Find key range on shard0 with prefix `ds_user_id_kr_*`
5. Select target shard with fewest key ranges
6. Redistribute key range using REDISTRIBUTE command

## Testing

```bash
python3 -m unittest discover -s . -p "test_*.py" -v
```

Includes 25 unit and integration tests.

## Requirements

- Python 3.10+
- psql
- tmux
