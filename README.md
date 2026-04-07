# pulsare

[![CI](https://github.com/ElEscribanoSilente/pulsare/actions/workflows/ci.yml/badge.svg)](https://github.com/ElEscribanoSilente/pulsare/actions/workflows/ci.yml)
[![Python](https://img.shields.io/pypi/pyversions/pulsare)](https://pypi.org/project/pulsare/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Task scheduling without infrastructure. One file. Zero deps.

```
pip install pulsare
```

## Why pulsare?

| Feature | schedule | APScheduler | celery beat | **pulsare** |
|---|---|---|---|---|
| Cron expressions | - | + | + | + |
| Async support | - | + | - | + |
| Concurrent execution | - | + | + | + |
| Retries + backoff | - | - | + | + |
| Timeouts | - | - | - | + |
| Persistence | - | + | + | + |
| Zero dependencies | + | - | - | **+** |
| Single file | + | - | - | **+** |

## Quick start

```python
from pulsare import Scheduler, every, cron, once

s = Scheduler()

@s.job(every(30).seconds, retries=3, timeout=10)
def health_check():
    ping_service()

@s.job(every().day.at("09:00"), missed="run_once")
def daily_report():
    send_report()

@s.job(cron("*/5 * * * *"), max_instances=2)
def rotate_cache():
    do_rotation()

s.run()  # blocking loop (Ctrl+C to stop)
```

## Fluent interval API

```python
every(5).seconds
every(2).minutes
every(1).hours
every(3).days
every().day.at("09:00")
every().monday.at("08:30")
every().friday.at("17:00:00")  # HH:MM:SS supported
```

## Cron expressions

Standard 5-field cron: `minute hour day-of-month month day-of-week`

```python
cron("*/5 * * * *")     # every 5 minutes
cron("0 9 * * 1-5")     # weekdays at 9am
cron("30 2 1 * *")      # 1st of month at 2:30am
cron("0 0 29 2 *")      # Feb 29 only (leap years)
```

## One-shot tasks

```python
once(after=30)   # run once, 30 seconds from now
once(after=0)    # run once, immediately
```

## Concurrency

```python
s = Scheduler(max_workers=4)

@s.job(every(10).seconds, max_instances=2)
def my_task():
    slow_operation()  # up to 2 running in parallel
```

## Retries with backoff

```python
@s.job(every(1).minutes, retries=3, retry_delay=2, retry_backoff=2)
def flaky_task():
    call_external_api()  # retries at 2s, 4s, 8s
```

## Timeouts

```python
@s.job(every(1).minutes, timeout=30, max_consecutive_timeouts=3)
def risky_task(cancel_event=None):
    while not cancel_event.is_set():  # cooperative cancellation
        do_chunk()
```

Jobs that exceed `max_consecutive_timeouts` are automatically paused.

## Lifecycle hooks

```python
@s.job(
    every(5).minutes,
    on_success=lambda name, val, dur: log(f"{name} ok in {dur:.1f}s"),
    on_failure=lambda name, exc, dur: alert(f"{name} failed: {exc}"),
    on_timeout=lambda name, dur: alert(f"{name} timed out"),
)
def monitored_task():
    return process_data()
```

## Metrics

```python
m = s.get_metrics("my_task")
# {'success_count': 42, 'fail_count': 1, 'timeout_count': 0,
#  'retry_count': 3, 'total_runs': 43, 'avg_duration': 1.23, ...}

all_m = s.all_metrics()  # dict of all jobs
```

## Tags, pause, cancel

```python
j = s.add(my_fn, every(1).minutes, tags={"api", "critical"})

s.pause("api")              # pause all jobs tagged "api"
s.resume("api")             # resume them
s.cancel("api")             # cancel all tagged "api"
s.cancel_by_name("my_fn")   # cancel by name
s.purge()                   # remove cancelled jobs
```

## Persistence (SQLite)

```python
from pulsare import Scheduler, SQLiteStore, every

store = SQLiteStore("jobs.db")  # WAL mode, zero config
s = Scheduler(store=store)

@s.job(every(5).minutes)
def my_task():
    do_work()

s.run()  # state survives restarts
```

Persists: next_run, metrics, paused flag. Jobs matched by name on restart.

## Async

```python
import asyncio
from pulsare import Scheduler, every

s = Scheduler()

@s.job(every(10).seconds, timeout=5)
async def async_task():
    async with aiohttp.ClientSession() as session:
        await session.get("https://api.example.com")

asyncio.run(s.run_async())  # native async loop
```

Async jobs share a persistent event loop -- connection pools, caches, etc. work correctly.

## Missed job policies

```python
from pulsare import MissedPolicy

@s.job(every(1).hours, missed="skip")      # default: reschedule to next slot
@s.job(every(1).hours, missed="run_once")  # execute once, then resume
@s.job(every(1).hours, missed="run_all")   # catch up all missed intervals
```

## Timezone

```python
from zoneinfo import ZoneInfo

s = Scheduler(tz=ZoneInfo("America/New_York"))
# All jobs use this timezone for .at() and cron expressions
```

System timezone detected automatically (Windows registry, /etc/localtime, $TZ).

## Requirements

- Python >= 3.10
- Zero external dependencies

## License

MIT
