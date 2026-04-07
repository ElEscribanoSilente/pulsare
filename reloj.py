"""
reloj.py — Task scheduling without infrastructure. One file. Zero deps.

Design trade-offs vs. alternatives:
  - schedule: no cron, no async, no concurrency, no retries, no timeouts
  - APScheduler: full-featured but 15K+ lines, requires SQLAlchemy for stores
  - celery beat: requires broker (Redis/RabbitMQ), massive infrastructure

reloj balances simplicity with real features:
  OK every(5).seconds / every(2).hours / every().monday fluent API
  OK Cron-style expressions: cron("*/5 * * * *")
  OK One-shot delayed tasks: once(after=30)
  OK Concurrent execution with thread pool (opt-in)
  OK Job tagging, pause/resume, cancel
  OK Configurable max concurrent instances per job (overlap control)
  OK Retry with exponential backoff
  OK Execution timeout (detects hung jobs; see _run_with_timeout caveats)
  OK Missed job policy (skip / run_once / run_all)
  OK Per-job lifecycle hooks (on_success / on_failure / on_timeout)
  OK Built-in metrics (duration, success/fail counts, last_error)
  OK Async-compatible scheduler loop (offloads ticks to executor)
  OK Context manager support
  OK Configurable error handling & jitter
  OK Timezone-aware scheduling (system local tz by default)
  OK Pluggable persistence via JobStore protocol
  OK Built-in SQLiteStore (WAL mode, zero external deps)
  OK ~900 lines of logic, zero external dependencies

Usage:
    from reloj import Scheduler, every, cron, once

    s = Scheduler()

    @s.job(every(30).seconds, retries=3, timeout=10)
    def health_check():
        ping_service()

    @s.job(every().day.at("09:00"), missed="run_once")
    def daily_report():
        send_report()

    @s.job(cron("*/5 * * * *"), max_instances=2)
    def five_minutes():
        rotate_cache()

    s.run()  # blocking loop (Ctrl+C to stop)

Persistence:
    from reloj import Scheduler, SQLiteStore, every

    store = SQLiteStore("my_jobs.db")
    s = Scheduler(store=store)

    @s.job(every(5).minutes)
    def my_task():
        do_work()

    s.run()  # state survives restarts — next_run, metrics, paused flag

License: MIT
Author: Independent — not affiliated with any framework.
"""

from __future__ import annotations

import asyncio
import bisect
import calendar
import enum
import inspect
import json
import logging
import os
import random
import sqlite3
import sys
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Any, Callable, Optional, Protocol, Union, runtime_checkable

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None  # type: ignore[assignment,misc]

__version__ = "1.0.0"
__all__ = [
    "Scheduler", "every", "cron", "once", "JobResult",
    "MissedPolicy", "JobMetrics", "JobStore", "SQLiteStore",
]

logger = logging.getLogger("reloj")

# ─── Enums ────────────────────────────────────────────────────────────────────


class MissedPolicy(enum.Enum):
    """What to do when a job's scheduled time has long passed.

    SKIP:     Reschedule to the next future slot. Default — safest.
    RUN_ONCE: Execute once immediately, then resume normal schedule.
    RUN_ALL:  Execute once for every missed interval, then resume. Use with care.
    """
    SKIP = "skip"
    RUN_ONCE = "run_once"
    RUN_ALL = "run_all"


# ─── Type alias ───────────────────────────────────────────────────────────────

ScheduleSpec = Union["_Interval", "_CronSpec", "_OnceSpec"]

# ─── Timezone helper ──────────────────────────────────────────────────────────


_WIN_TZ_MAP: dict[str, str] = {
    "Romance Standard Time": "Europe/Paris",
    "W. Europe Standard Time": "Europe/Berlin",
    "Central European Standard Time": "Europe/Warsaw",
    "Eastern Standard Time": "America/New_York",
    "Central Standard Time": "America/Chicago",
    "Mountain Standard Time": "America/Denver",
    "Pacific Standard Time": "America/Los_Angeles",
    "GMT Standard Time": "Europe/London",
    "Tokyo Standard Time": "Asia/Tokyo",
    "China Standard Time": "Asia/Shanghai",
    "India Standard Time": "Asia/Kolkata",
    "AUS Eastern Standard Time": "Australia/Sydney",
    "SA Pacific Standard Time": "America/Bogota",
    "Argentina Standard Time": "America/Buenos_Aires",
    "E. South America Standard Time": "America/Sao_Paulo",
    "Central Standard Time (Mexico)": "America/Mexico_City",
    "US Mountain Standard Time": "America/Phoenix",
    "Atlantic Standard Time": "America/Halifax",
    "Montevideo Standard Time": "America/Montevideo",
    "Cuba Standard Time": "America/Havana",
}


def _windows_tz_name() -> str | None:
    """Read the Windows registry for the current timezone and map to IANA."""
    try:
        import winreg  # type: ignore[import-untyped]
        key = winreg.OpenKey(
            winreg.HKEY_LOCAL_MACHINE,
            r"SYSTEM\CurrentControlSet\Control\TimeZoneInformation",
        )
        win_name, _ = winreg.QueryValueEx(key, "TimeZoneKeyName")
        winreg.CloseKey(key)
        return _WIN_TZ_MAP.get(win_name, win_name)
    except (OSError, ImportError, FileNotFoundError):
        return None


_CACHED_SYSTEM_TZ: tzinfo | None = None


def _system_tz() -> tzinfo:
    """Return the system's *named* timezone (DST-aware) when possible.

    Result is cached at module level — resolved once on first call.
    """
    global _CACHED_SYSTEM_TZ
    if _CACHED_SYSTEM_TZ is not None:
        return _CACHED_SYSTEM_TZ
    tz: tzinfo | None = None
    if ZoneInfo is not None:
        tz_name = os.environ.get("TZ")
        if not tz_name and sys.platform == "win32":
            tz_name = _windows_tz_name()
        if not tz_name and sys.platform != "win32":
            try:
                link = os.readlink("/etc/localtime")
                idx = link.find("zoneinfo/")
                if idx != -1:
                    tz_name = link[idx + len("zoneinfo/"):]
            except (OSError, ValueError):
                pass
        if tz_name:
            try:
                tz = ZoneInfo(tz_name)
            except (KeyError, Exception):
                pass
    if tz is None:
        # Fallback: fixed offset (no DST transitions)
        logger.warning(
            "Could not determine named timezone; using fixed UTC offset (no DST). "
            "Set the TZ environment variable for DST support."
        )
        tz = datetime.now(timezone.utc).astimezone().tzinfo  # type: ignore[assignment]
    _CACHED_SYSTEM_TZ = tz
    return tz  # type: ignore[return-value]


def _now_tz(tz: tzinfo | None = None) -> datetime:
    return datetime.now(tz or _system_tz())


# ─── Schedule specs ───────────────────────────────────────────────────────────

_UNIT_SECONDS = {
    "seconds": 1, "minutes": 60, "hours": 3600, "days": 86400, "weeks": 604800,
}

_DAY_MAP = {
    "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
    "friday": 4, "saturday": 5, "sunday": 6,
}


class _Interval:
    __slots__ = ("_n", "_unit", "_at_time", "_weekday", "_tz")

    def __init__(self, n: int = 1, tz: tzinfo | None = None):
        self._n = n
        self._unit: str = ""
        self._at_time: str | None = None
        self._weekday: int | None = None
        self._tz = tz

    def _clone(self, **overrides: Any) -> _Interval:
        cp = _Interval(self._n, self._tz)
        cp._unit = overrides.get("_unit", self._unit)
        cp._at_time = overrides.get("_at_time", self._at_time)
        cp._weekday = overrides.get("_weekday", self._weekday)
        return cp

    def _set_unit(self, u: str) -> _Interval:
        return self._clone(_unit=u)

    @property
    def seconds(self) -> _Interval: return self._set_unit("seconds")
    @property
    def minutes(self) -> _Interval: return self._set_unit("minutes")
    @property
    def hours(self) -> _Interval: return self._set_unit("hours")
    @property
    def days(self) -> _Interval: return self._set_unit("days")

    @property
    def day(self) -> _Interval:
        if self._n != 1:
            raise ValueError(
                f"every({self._n}).day is ambiguous — use every({self._n}).days "
                f"for a multi-day interval, or every().day for daily scheduling."
            )
        return self._set_unit("days")

    def _set_weekday(self, wd: int) -> _Interval:
        return self._clone(_weekday=wd, _unit="weeks")

    @property
    def monday(self) -> _Interval: return self._set_weekday(0)
    @property
    def tuesday(self) -> _Interval: return self._set_weekday(1)
    @property
    def wednesday(self) -> _Interval: return self._set_weekday(2)
    @property
    def thursday(self) -> _Interval: return self._set_weekday(3)
    @property
    def friday(self) -> _Interval: return self._set_weekday(4)
    @property
    def saturday(self) -> _Interval: return self._set_weekday(5)
    @property
    def sunday(self) -> _Interval: return self._set_weekday(6)

    def at(self, time_str: str) -> _Interval:
        if not _validate_time_str(time_str):
            raise ValueError(f"Invalid time format: '{time_str}'. Use HH:MM or HH:MM:SS")
        if self._unit and self._unit not in ("days", "weeks") and self._weekday is None:
            raise ValueError(f".at() is only valid with .day/.days or weekdays, not .{self._unit}")
        return self._clone(_at_time=time_str)

    def to_seconds(self) -> float:
        if not self._unit:
            raise ValueError("Interval unit not set.")
        return self._n * _UNIT_SECONDS[self._unit]

    def next_run(self) -> float:
        if self._weekday is not None:
            return _next_weekday_at(self._weekday, self._at_time or "00:00", self._tz)
        elif self._at_time and self._unit == "days":
            return _next_daily_at(self._at_time, self._tz)
        return time.time() + self.to_seconds()

    def __repr__(self) -> str:
        parts = [f"every({self._n})"]
        if self._weekday is not None:
            parts.append([k for k, v in _DAY_MAP.items() if v == self._weekday][0])
        elif self._unit:
            parts.append(self._unit)
        if self._at_time:
            parts.append(f"at('{self._at_time}')")
        return ".".join(parts)


class _CronSpec:
    __slots__ = ("_expr", "_minute", "_hour", "_dom", "_month", "_dow", "_tz")
    _MAX_HORIZON = timedelta(days=366 * 4)

    def __init__(self, expr: str, tz: tzinfo | None = None):
        self._expr = expr
        self._tz = tz
        parts = expr.strip().split()
        if len(parts) != 5:
            raise ValueError(f"Cron needs 5 fields, got {len(parts)}: '{expr}'")
        self._minute = self._parse_field(parts[0], 0, 59)
        self._hour = self._parse_field(parts[1], 0, 23)
        self._dom = self._parse_field(parts[2], 1, 31)
        self._month = self._parse_field(parts[3], 1, 12)
        self._dow = self._parse_field(parts[4], 0, 6)
        self._validate_reachable()

    def _validate_reachable(self) -> None:
        """Reject expressions where no DOM fits any specified month.

        NOTE: this does NOT cross-check DOW.  ``0 0 31 6 1`` (Monday the
        31st of June) passes validation even though June has 30 days.
        Full reachability would require iterating real calendar years,
        which is what next_run() already does — _validate_reachable only
        catches the *obvious* impossibilities (DOM × month).
        """
        for month in self._month:
            max_day = 29 if month == 2 else calendar.monthrange(2000, month)[1]
            if any(d <= max_day for d in self._dom):
                return
        raise ValueError(
            f"Cron '{self._expr}' can never match: "
            f"no day in {sorted(self._dom)} exists in months {sorted(self._month)}"
        )

    @staticmethod
    def _parse_field(raw: str, lo: int, hi: int) -> set[int]:
        vals: set[int] = set()
        for part in raw.split(","):
            if part == "*":
                vals.update(range(lo, hi + 1))
            elif "/" in part:
                base, step_s = part.split("/", 1)
                start = lo if base in ("*", "") else int(base)
                step = int(step_s)
                if step <= 0:
                    raise ValueError(f"Step must be positive, got {step}")
                vals.update(range(start, hi + 1, step))
            elif "-" in part:
                a, b = part.split("-", 1)
                a_i, b_i = int(a), int(b)
                if a_i > b_i:
                    raise ValueError(f"Invalid range: {a_i}-{b_i}")
                vals.update(range(a_i, b_i + 1))
            else:
                v = int(part)
                if not (lo <= v <= hi):
                    raise ValueError(f"Value {v} out of [{lo}, {hi}]")
                vals.add(v)
        return vals

    @staticmethod
    def _next_ge(sorted_vals: list[int], v: int) -> int | None:
        """Return first element >= v via binary search, or None."""
        idx = bisect.bisect_left(sorted_vals, v)
        return sorted_vals[idx] if idx < len(sorted_vals) else None

    def next_run(self) -> float:
        """Find next matching datetime using field-level jumps.

        Instead of iterating every minute (O(n^4) worst case), each field
        jumps directly to its next valid value via _next_ge().  When a
        field wraps, the parent is advanced and children are reset.
        Worst case iterates ~days-in-horizon for DOW filtering.
        """
        now = _now_tz(self._tz)
        cand = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
        limit = now + self._MAX_HORIZON
        sm = sorted(self._month)
        sd = sorted(self._dom)
        sh = sorted(self._hour)
        smin = sorted(self._minute)
        tz = now.tzinfo

        y, mo, d, h, mi = cand.year, cand.month, cand.day, cand.hour, cand.minute

        # Safety bound: at most one iteration per day in horizon + margin
        for _ in range(366 * 4 + 50):
            # ── Month ──
            nmo = self._next_ge(sm, mo)
            if nmo is None:
                y += 1; mo = sm[0]; d = sd[0]; h = sh[0]; mi = smin[0]
                continue
            if nmo != mo:
                mo = nmo; d = sd[0]; h = sh[0]; mi = smin[0]

            # ── Day ──
            max_d = calendar.monthrange(y, mo)[1]
            nd = self._next_ge(sd, d)
            if nd is None or nd > max_d:
                nmo2 = self._next_ge(sm, mo + 1)
                if nmo2 is None:
                    y += 1; mo = sm[0]
                else:
                    mo = nmo2
                d = sd[0]; h = sh[0]; mi = smin[0]
                continue
            if nd != d:
                d = nd; h = sh[0]; mi = smin[0]

            # ── DOW check ──
            try:
                wd = datetime(y, mo, d, tzinfo=tz).weekday()
            except ValueError:
                nd2 = self._next_ge(sd, d + 1)
                if nd2 is None or nd2 > max_d:
                    nmo2 = self._next_ge(sm, mo + 1)
                    if nmo2 is None:
                        y += 1; mo = sm[0]
                    else:
                        mo = nmo2
                    d = sd[0]
                else:
                    d = nd2
                h = sh[0]; mi = smin[0]
                continue

            if wd not in self._dow:
                nd2 = self._next_ge(sd, d + 1)
                if nd2 is None or nd2 > max_d:
                    nmo2 = self._next_ge(sm, mo + 1)
                    if nmo2 is None:
                        y += 1; mo = sm[0]
                    else:
                        mo = nmo2
                    d = sd[0]
                else:
                    d = nd2
                h = sh[0]; mi = smin[0]
                continue

            # ── Hour ──
            nh = self._next_ge(sh, h)
            if nh is None:
                nd2 = self._next_ge(sd, d + 1)
                if nd2 is None or nd2 > max_d:
                    nmo2 = self._next_ge(sm, mo + 1)
                    if nmo2 is None:
                        y += 1; mo = sm[0]
                    else:
                        mo = nmo2
                    d = sd[0]
                else:
                    d = nd2
                h = sh[0]; mi = smin[0]
                continue
            if nh != h:
                h = nh; mi = smin[0]

            # ── Minute ──
            nmi = self._next_ge(smin, mi)
            if nmi is None:
                h += 1; mi = smin[0]
                continue
            mi = nmi

            # ── Build result ──
            dt = datetime(y, mo, d, h, mi, tzinfo=tz)
            if dt > limit:
                break
            return dt.timestamp()

        raise ValueError(f"Cron '{self._expr}': no match within horizon")

    def __repr__(self) -> str:
        return f"cron('{self._expr}')"


class _OnceSpec:
    __slots__ = ("_after",)

    def __init__(self, after: float):
        if after < 0:
            raise ValueError(f"Delay must be non-negative, got {after}")
        self._after = after

    def next_run(self) -> float:
        return time.time() + self._after

    def __repr__(self) -> str:
        return f"once(after={self._after})"


# ─── Factory functions ────────────────────────────────────────────────────────

def every(n: int = 1) -> _Interval:
    if n <= 0:
        raise ValueError(f"Interval count must be positive, got {n}")
    return _Interval(n)

def cron(expr: str) -> _CronSpec:
    return _CronSpec(expr)

def once(after: float) -> _OnceSpec:
    return _OnceSpec(after)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _validate_time_str(s: str) -> bool:
    parts = s.split(":")
    if len(parts) not in (2, 3):
        return False
    try:
        h, m = int(parts[0]), int(parts[1])
        sec = int(parts[2]) if len(parts) == 3 else 0
        return 0 <= h <= 23 and 0 <= m <= 59 and 0 <= sec <= 59
    except ValueError:
        return False

def _parse_time_str(s: str) -> tuple[int, int, int]:
    parts = s.split(":")
    return int(parts[0]), int(parts[1]), int(parts[2]) if len(parts) == 3 else 0

def _next_daily_at(time_str: str, tz: tzinfo | None = None) -> float:
    h, m, sec = _parse_time_str(time_str)
    now = _now_tz(tz)
    target = now.replace(hour=h, minute=m, second=sec, microsecond=0)
    if target.timestamp() <= time.time():
        target += timedelta(days=1)
    return target.timestamp()

def _next_weekday_at(weekday: int, time_str: str, tz: tzinfo | None = None) -> float:
    h, m, sec = _parse_time_str(time_str)
    now = _now_tz(tz)
    days_ahead = (weekday - now.weekday()) % 7
    target = (now + timedelta(days=days_ahead)).replace(hour=h, minute=m, second=sec, microsecond=0)
    if target.timestamp() <= time.time():
        target += timedelta(days=7)
    return target.timestamp()


# ─── Timeout ──────────────────────────────────────────────────────────────────

class _JobTimeoutError(Exception):
    """Raised when a job exceeds its timeout."""

_abandoned_threads: list[threading.Thread] = []
_abandoned_lock = threading.Lock()


def _track_abandoned(t: threading.Thread) -> None:
    """Track an abandoned thread and clean up any that have finished."""
    with _abandoned_lock:
        _abandoned_threads.append(t)
        # Purge dead threads while we're here
        still_alive = [th for th in _abandoned_threads if th.is_alive()]
        dead_count = len(_abandoned_threads) - len(still_alive)
        _abandoned_threads[:] = still_alive
    if dead_count > 0:
        logger.debug("Reaped %d finished abandoned thread(s), %d still alive", dead_count, len(still_alive))
    if len(still_alive) >= 5:
        logger.warning(
            "%d abandoned threads still alive — jobs with timeouts may be leaking resources. "
            "Consider using cooperative cancellation (cancel_event).",
            len(still_alive),
        )


def _run_with_timeout(fn: Callable, timeout: float, cancel_event: threading.Event | None = None) -> Any:
    """Run *fn* in a daemon thread; raise _JobTimeoutError if exceeded.

    CAVEAT: Python cannot forcibly kill a thread.  On timeout the thread is
    **abandoned** — it keeps running until it finishes or the process exits.
    The optional *cancel_event* is set on timeout so cooperative jobs can
    check ``event.is_set()`` and bail out.  Abandoned threads are tracked
    and logged so resource leaks are visible.
    """
    result_box: list[Any] = []
    error_box: list[BaseException] = []
    done = threading.Event()

    def wrapper() -> None:
        try:
            result_box.append(fn())
        except BaseException as e:
            error_box.append(e)
        finally:
            done.set()

    t = threading.Thread(target=wrapper, daemon=True, name=f"reloj-timeout-{id(fn):#x}")
    t.start()
    if not done.wait(timeout=timeout):
        if cancel_event is not None:
            cancel_event.set()
        _track_abandoned(t)
        raise _JobTimeoutError(f"Exceeded {timeout}s timeout (thread '{t.name}' abandoned)")
    if error_box:
        raise error_box[0]
    return result_box[0] if result_box else None


# ─── Metrics ──────────────────────────────────────────────────────────────────

@dataclass
class JobMetrics:
    success_count: int = 0
    fail_count: int = 0
    timeout_count: int = 0
    retry_count: int = 0
    total_duration: float = 0.0
    last_duration: float = 0.0
    last_error: str | None = None
    last_success_at: float = 0.0
    last_failure_at: float = 0.0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def record_success(self, duration: float) -> None:
        with self._lock:
            self.success_count += 1
            self.total_duration += duration
            self.last_duration = duration
            self.last_success_at = time.time()

    def record_failure(self, duration: float, error: str) -> None:
        with self._lock:
            self.fail_count += 1
            self.total_duration += duration
            self.last_duration = duration
            self.last_error = error
            self.last_failure_at = time.time()

    def record_timeout(self, duration: float) -> None:
        with self._lock:
            self.timeout_count += 1
            self.total_duration += duration
            self.last_duration = duration
            self.last_error = "timeout"
            self.last_failure_at = time.time()

    def record_retry(self) -> None:
        with self._lock:
            self.retry_count += 1

    @property
    def avg_duration(self) -> float:
        with self._lock:
            total = self.success_count + self.fail_count + self.timeout_count
            return self.total_duration / total if total > 0 else 0.0

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            total = self.success_count + self.fail_count + self.timeout_count
            return {
                "success_count": self.success_count,
                "fail_count": self.fail_count,
                "timeout_count": self.timeout_count,
                "retry_count": self.retry_count,
                "total_runs": total,
                "avg_duration": self.total_duration / total if total > 0 else 0.0,
                "last_duration": self.last_duration,
                "last_error": self.last_error,
            }


# ─── Job Store ────────────────────────────────────────────────────────────────


@runtime_checkable
class JobStore(Protocol):
    """Protocol for pluggable job persistence backends."""

    def load(self, name: str) -> dict[str, Any] | None:
        """Load persisted state for a job by name. Returns None if not found."""
        ...

    def save(self, name: str, state: dict[str, Any]) -> None:
        """Persist job state (next_run, metrics, paused, etc.)."""
        ...

    def delete(self, name: str) -> None:
        """Remove a job's persisted state."""
        ...

    def all_names(self) -> list[str]:
        """Return all persisted job names."""
        ...

    def close(self) -> None:
        """Release resources (connections, file handles)."""
        ...


class SQLiteStore:
    """SQLite-backed job store. WAL mode for concurrent reads. Zero external deps.

    Persists job *state* (schedule position, metrics, paused flag), not code.
    Jobs are matched by name — re-register the same functions on restart and
    the store restores where each job left off.

    Usage:
        store = SQLiteStore("my_jobs.db")
        s = Scheduler(store=store)

        @s.job(every(5).minutes)
        def my_task(): ...

        s.run()  # on restart, my_task resumes from its last next_run
    """

    _SCHEMA = """
    CREATE TABLE IF NOT EXISTS jobs (
        name            TEXT PRIMARY KEY,
        next_run        REAL NOT NULL,
        last_run        REAL NOT NULL DEFAULT 0,
        run_count       INTEGER NOT NULL DEFAULT 0,
        paused          INTEGER NOT NULL DEFAULT 0,
        success_count   INTEGER NOT NULL DEFAULT 0,
        fail_count      INTEGER NOT NULL DEFAULT 0,
        timeout_count   INTEGER NOT NULL DEFAULT 0,
        retry_count     INTEGER NOT NULL DEFAULT 0,
        total_duration  REAL NOT NULL DEFAULT 0,
        last_duration   REAL NOT NULL DEFAULT 0,
        last_error      TEXT,
        last_success_at REAL NOT NULL DEFAULT 0,
        last_failure_at REAL NOT NULL DEFAULT 0,
        created_at      REAL NOT NULL,
        updated_at      REAL NOT NULL
    );
    """

    def __init__(self, path: str = "reloj_jobs.db") -> None:
        self._path = path
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(self._SCHEMA)
        self._lock = threading.Lock()

    def load(self, name: str) -> dict[str, Any] | None:
        with self._lock:
            cur = self._conn.execute(
                "SELECT * FROM jobs WHERE name = ?", (name,)
            )
            row = cur.fetchone()
            if row is None:
                return None
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))

    def save(self, name: str, state: dict[str, Any]) -> None:
        now = time.time()
        with self._lock:
            self._conn.execute(
                """INSERT INTO jobs (
                    name, next_run, last_run, run_count, paused,
                    success_count, fail_count, timeout_count,
                    retry_count, total_duration, last_duration,
                    last_error, last_success_at, last_failure_at,
                    created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(name) DO UPDATE SET
                    next_run=excluded.next_run, last_run=excluded.last_run,
                    run_count=excluded.run_count, paused=excluded.paused,
                    success_count=excluded.success_count, fail_count=excluded.fail_count,
                    timeout_count=excluded.timeout_count, retry_count=excluded.retry_count,
                    total_duration=excluded.total_duration, last_duration=excluded.last_duration,
                    last_error=excluded.last_error, last_success_at=excluded.last_success_at,
                    last_failure_at=excluded.last_failure_at, updated_at=excluded.updated_at
                """,
                (
                    name, state["next_run"], state["last_run"],
                    state["run_count"], int(state.get("paused", False)),
                    state["success_count"], state["fail_count"],
                    state["timeout_count"], state["retry_count"],
                    state["total_duration"], state["last_duration"],
                    state.get("last_error"), state["last_success_at"],
                    state["last_failure_at"], now, now,
                ),
            )
            self._conn.commit()

    def delete(self, name: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM jobs WHERE name = ?", (name,))
            self._conn.commit()

    def all_names(self) -> list[str]:
        with self._lock:
            rows = self._conn.execute("SELECT name FROM jobs").fetchall()
            return [r[0] for r in rows]

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __repr__(self) -> str:
        return f"SQLiteStore('{self._path}')"


def _job_to_state(job: "_Job") -> dict[str, Any]:
    """Extract persistable state from a _Job instance."""
    m = job.metrics
    with m._lock:
        return {
            "next_run": job.next_run,
            "last_run": job.last_run,
            "run_count": job.run_count,
            "paused": job.paused,
            "success_count": m.success_count,
            "fail_count": m.fail_count,
            "timeout_count": m.timeout_count,
            "retry_count": m.retry_count,
            "total_duration": m.total_duration,
            "last_duration": m.last_duration,
            "last_error": m.last_error,
            "last_success_at": m.last_success_at,
            "last_failure_at": m.last_failure_at,
        }


def _restore_job_state(job: "_Job", state: dict[str, Any]) -> None:
    """Restore persisted state into a _Job instance."""
    job.next_run = state["next_run"]
    job.last_run = state["last_run"]
    job.run_count = state["run_count"]
    job.paused = bool(state["paused"])

    m = job.metrics
    with m._lock:
        m.success_count = state["success_count"]
        m.fail_count = state["fail_count"]
        m.timeout_count = state["timeout_count"]
        m.retry_count = state["retry_count"]
        m.total_duration = state["total_duration"]
        m.last_duration = state["last_duration"]
        m.last_error = state.get("last_error")
        m.last_success_at = state["last_success_at"]
        m.last_failure_at = state["last_failure_at"]

    # Handle missed schedule: if next_run is in the past, let missed_policy decide
    if job.next_run < time.time() and not job.paused:
        if job.missed_policy == MissedPolicy.SKIP:
            job.reschedule()
            logger.info("Job '%s' missed schedule, rescheduled (SKIP policy)", job.name)
        else:
            logger.info(
                "Job '%s' missed schedule (%.1fs overdue), will execute per %s policy",
                job.name, time.time() - job.next_run, job.missed_policy.value,
            )


# ─── Cancel-event detection ──────────────────────────────────────────────────

def _detect_cancel_param(fn: Callable) -> bool:
    """Safely detect if *fn* accepts a ``cancel_event`` keyword argument.

    Security hardening:
      - Catches ALL exceptions (a malicious __signature__ property could
        execute arbitrary code or raise unexpected exceptions).
      - Validates that the detected parameter actually accepts keyword
        passing (POSITIONAL_OR_KEYWORD or KEYWORD_ONLY).
      - Time-bounded: inspect.signature is called once at job creation,
        never during hot execution paths.
    """
    try:
        sig = inspect.signature(fn)
        param = sig.parameters.get("cancel_event")
        if param is None:
            return False
        # Only inject via keyword — reject VAR_POSITIONAL (*args) etc.
        return param.kind in (
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        )
    except Exception:
        # Any failure (ValueError, TypeError, RecursionError, malicious
        # __signature__ descriptors, etc.) → safe default: don't inject.
        return False


# ─── Job ──────────────────────────────────────────────────────────────────────

@dataclass
class _Job:
    fn: Callable
    spec: ScheduleSpec
    tags: set[str] = field(default_factory=set)
    name: str = ""
    next_run: float = 0.0
    last_run: float = 0.0
    run_count: int = 0
    paused: bool = False
    one_shot: bool = False
    jitter: float = 0.0
    max_instances: int = 1
    retries: int = 0
    retry_delay: float = 1.0
    retry_backoff: float = 2.0
    timeout: float = 0.0
    missed_policy: MissedPolicy = MissedPolicy.SKIP
    max_catchup: int = 1000
    max_consecutive_timeouts: int = 0
    on_success: Callable[[str, Any, float], None] | None = None
    on_failure: Callable[[str, Exception, float], None] | None = None
    on_timeout: Callable[[str, float], None] | None = None
    metrics: JobMetrics = field(default_factory=JobMetrics)
    _cancelled: bool = field(default=False, init=False, repr=False)
    _instance_count: int = field(default=0, init=False, repr=False)
    _instance_lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _cancel_event: threading.Event = field(default_factory=threading.Event, init=False, repr=False)
    _active_events: list[threading.Event] = field(default_factory=list, init=False, repr=False)
    _events_lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _is_async: bool = field(default=False, init=False, repr=False)
    _wants_cancel: bool = field(default=False, init=False, repr=False)
    _consecutive_timeouts: int = field(default=0, init=False, repr=False)

    def __post_init__(self) -> None:
        if not self.name:
            self.name = getattr(self.fn, "__name__", "anonymous")
        if self.max_instances < 1:
            raise ValueError(f"max_instances must be >= 1, got {self.max_instances}")
        self.next_run = self.spec.next_run()
        self.one_shot = isinstance(self.spec, _OnceSpec)
        self._is_async = asyncio.iscoroutinefunction(self.fn)
        self._wants_cancel = _detect_cancel_param(self.fn)

    def __copy__(self) -> None:
        raise TypeError(
            f"Cannot copy _Job '{self.name}': contains locks, events, and "
            f"concurrency state that cannot be safely duplicated."
        )

    def __deepcopy__(self, memo: Any) -> None:
        raise TypeError(
            f"Cannot deepcopy _Job '{self.name}': contains locks, events, and "
            f"concurrency state that cannot be safely duplicated."
        )

    def reschedule(self) -> None:
        if isinstance(self.spec, _Interval):
            if self.spec._at_time and (self.spec._unit == "days" or self.spec._weekday is not None):
                self.next_run = self.spec.next_run()
            else:
                # Anchor-based: advance from the scheduled time, not wall clock.
                # Prevents cumulative drift when job execution takes time.
                # Clamp to now so we never schedule into the past.
                anchor = self.next_run + self.spec.to_seconds()
                self.next_run = max(anchor, time.time() + 0.001)
        elif isinstance(self.spec, _CronSpec):
            self.next_run = self.spec.next_run()
        if self.jitter > 0:
            self.next_run += random.uniform(0, self.jitter)

    def cancel(self) -> None:
        self._cancelled = True
        self._cancel_event.set()
        with self._events_lock:
            for evt in self._active_events:
                evt.set()

    def acquire_instance(self) -> bool:
        with self._instance_lock:
            if self._instance_count >= self.max_instances:
                return False
            self._instance_count += 1
            return True

    def release_instance(self, *, update_stats: bool = False) -> None:
        with self._instance_lock:
            self._instance_count = max(0, self._instance_count - 1)
            if update_stats:
                self.last_run = time.time()
                self.run_count += 1

    @property
    def is_due(self) -> bool:
        return not self._cancelled and not self.paused and time.time() >= self.next_run

    @property
    def is_running(self) -> bool:
        with self._instance_lock:
            return self._instance_count > 0

    def missed_intervals(self) -> int:
        if isinstance(self.spec, _Interval) and self.spec._at_time is None:
            interval = self.spec.to_seconds()
            if interval > 0:
                elapsed = time.time() - self.next_run
                raw = max(0, int(elapsed / interval)) if elapsed > 0 else 0
                if raw > self.max_catchup:
                    logger.warning(
                        "Job '%s' missed %d intervals, capping to max_catchup=%d",
                        self.name, raw, self.max_catchup,
                    )
                return min(raw, self.max_catchup)
        return 0


# ─── Job result ───────────────────────────────────────────────────────────────

class JobResult:
    __slots__ = ("_future", "_value", "_exception", "_done")

    def __init__(self, future: Future | None = None) -> None:
        self._future = future
        self._value: Any = None
        self._exception: BaseException | None = None
        self._done = False

    def _set_sync(self, value: Any = None, exception: BaseException | None = None) -> None:
        self._value = value
        self._exception = exception
        self._done = True

    def result(self, timeout: float | None = None) -> Any:
        if self._future is not None:
            return self._future.result(timeout=timeout)
        if self._exception:
            raise self._exception
        return self._value

    def exception(self, timeout: float | None = None) -> BaseException | None:
        if self._future is not None:
            return self._future.exception(timeout=timeout)
        return self._exception

    def done(self) -> bool:
        return self._future.done() if self._future is not None else self._done


# ─── Scheduler ────────────────────────────────────────────────────────────────

class Scheduler:
    """In-process task scheduler with retries, timeouts, metrics, and lifecycle hooks."""

    def __init__(
        self,
        max_workers: int = 0,
        tick: float = 1.0,
        on_error: Callable[[str, Exception], None] | None = None,
        jitter: float = 0.0,
        tz: tzinfo | None = None,
        store: JobStore | None = None,
    ):
        self._jobs: list[_Job] = []
        self._lock = threading.Lock()
        self._running = False
        self._tick = tick
        self._max_workers = max_workers
        self._pool: ThreadPoolExecutor | None = None
        self._on_error = on_error
        self._default_jitter = jitter
        self._tz = tz
        self._store = store
        self._async_loop: asyncio.AbstractEventLoop | None = None
        self._async_thread: threading.Thread | None = None

    def _ensure_async_loop(self) -> asyncio.AbstractEventLoop:
        """Return a persistent background event loop for async jobs in sync context.

        All async jobs dispatched from tick_once() (sync path) share this loop,
        so coroutines can share resources (connection pools, caches, etc.)
        instead of each call creating and destroying its own loop via asyncio.run().

        If the loop thread died (e.g. unhandled exception), a new one is spawned.
        """
        needs_new = (
            self._async_loop is None
            or self._async_loop.is_closed()
            or not self._async_loop.is_running()
        )
        if needs_new:
            if self._async_loop is not None and not self._async_loop.is_closed():
                self._async_loop.close()
            self._async_loop = asyncio.new_event_loop()
            self._async_thread = threading.Thread(
                target=self._async_loop.run_forever,
                daemon=True,
                name="reloj-async-loop",
            )
            self._async_thread.start()
            # Wait briefly for the loop to actually start running
            for _ in range(50):
                if self._async_loop.is_running():
                    break
                time.sleep(0.001)
        return self._async_loop

    def _shutdown_async_loop(self) -> None:
        if self._async_loop is not None and self._async_loop.is_running():
            self._async_loop.call_soon_threadsafe(self._async_loop.stop)
            if self._async_thread is not None:
                self._async_thread.join(timeout=5)
            self._async_loop.close()
        self._async_loop = None
        self._async_thread = None

    def _ensure_pool(self) -> ThreadPoolExecutor | None:
        if self._max_workers > 0 and self._pool is None:
            self._pool = ThreadPoolExecutor(max_workers=self._max_workers)
        return self._pool

    def __enter__(self) -> Scheduler:
        self._ensure_pool()
        return self

    def _cleanup_resources(self) -> None:
        """Release all scheduler resources: persist state, stop async loop,
        shut down thread pool, close store.  Idempotent."""
        self._persist_all()
        self._shutdown_async_loop()
        if self._pool:
            self._pool.shutdown(wait=True)
            self._pool = None
        if self._store:
            self._store.close()

    def __exit__(self, *exc: Any) -> None:
        self.stop()
        self._cleanup_resources()

    # ── Registration ──────────────────────────────────────────────────

    def _inject_tz(self, spec: ScheduleSpec) -> ScheduleSpec:
        """Clone spec and set scheduler timezone — never mutates the original."""
        if self._tz is not None:
            if isinstance(spec, _Interval):
                cp = spec._clone()
                cp._tz = self._tz
                return cp
            elif isinstance(spec, _CronSpec):
                cp = _CronSpec.__new__(_CronSpec)
                for slot in _CronSpec.__slots__:
                    setattr(cp, slot, getattr(spec, slot))
                cp._tz = self._tz
                return cp
        return spec

    def _make_job(self, fn: Callable, spec: ScheduleSpec, **kw: Any) -> _Job:
        spec = self._inject_tz(spec)
        missed = kw.pop("missed", MissedPolicy.SKIP)
        if isinstance(missed, str):
            missed = MissedPolicy(missed)
        jitter = kw.pop("jitter", None)
        return _Job(
            fn=fn, spec=spec,
            jitter=jitter if jitter is not None else self._default_jitter,
            missed_policy=missed,
            **kw,
        )

    def _restore_from_store(self, job: _Job) -> None:
        """If a store is configured, restore saved state into the job."""
        if self._store is None:
            return
        saved = self._store.load(job.name)
        if saved is not None:
            _restore_job_state(job, saved)
            logger.info("Job '%s' restored from store (next_run=%.1f)", job.name, job.next_run)
        else:
            self._store.save(job.name, _job_to_state(job))

    def _persist_job(self, job: _Job) -> None:
        """Persist current job state to the store."""
        if self._store is not None:
            try:
                self._store.save(job.name, _job_to_state(job))
            except Exception:
                logger.exception("Failed to persist job '%s'", job.name)

    def _persist_all(self) -> None:
        """Persist all active jobs to the store."""
        if self._store is None:
            return
        with self._lock:
            for j in self._jobs:
                if not j._cancelled:
                    self._persist_job(j)

    def _check_name_unique(self, name: str) -> None:
        """Raise ValueError if an active job with this name already exists."""
        for j in self._jobs:
            if j.name == name and not j._cancelled:
                raise ValueError(
                    f"Job name '{name}' already exists. Use a unique name or "
                    f"cancel the existing job first."
                )

    def job(self, spec: ScheduleSpec, **kw: Any) -> Callable:
        """Decorator to register a scheduled job."""
        def decorator(fn: Callable) -> Callable:
            kw.setdefault("name", fn.__name__)
            kw.setdefault("tags", set())
            j = self._make_job(fn, spec, **kw)
            self._restore_from_store(j)
            with self._lock:
                self._check_name_unique(j.name)
                self._jobs.append(j)
            return fn
        return decorator

    def add(self, fn: Callable, spec: ScheduleSpec, **kw: Any) -> _Job:
        """Programmatic job registration. Returns the Job for control."""
        kw.setdefault("name", getattr(fn, "__name__", "anonymous"))
        kw.setdefault("tags", set())
        j = self._make_job(fn, spec, **kw)
        self._restore_from_store(j)
        with self._lock:
            self._check_name_unique(j.name)
            self._jobs.append(j)
        return j

    # ── Control ───────────────────────────────────────────────────────

    def pause(self, tag: str) -> int:
        c = 0
        with self._lock:
            for j in self._jobs:
                if tag in j.tags:
                    j.paused = True; c += 1
                    self._persist_job(j)
        return c

    def resume(self, tag: str) -> int:
        c = 0
        with self._lock:
            for j in self._jobs:
                if tag in j.tags:
                    j.paused = False; c += 1
                    self._persist_job(j)
        return c

    def cancel(self, tag: str) -> int:
        c = 0
        with self._lock:
            for j in self._jobs:
                if tag in j.tags:
                    j.cancel(); c += 1
                    if self._store:
                        self._store.delete(j.name)
        return c

    def cancel_by_name(self, name: str) -> bool:
        with self._lock:
            for j in self._jobs:
                if j.name == name and not j._cancelled:
                    j.cancel()
                    if self._store:
                        self._store.delete(j.name)
                    return True
        return False

    def purge(self) -> int:
        with self._lock:
            before = len(self._jobs)
            purged = [j for j in self._jobs if j._cancelled and not j.is_running]
            self._jobs = [j for j in self._jobs if not (j._cancelled and not j.is_running)]
            if self._store:
                for j in purged:
                    self._store.delete(j.name)
            return before - len(self._jobs)

    @property
    def pending(self) -> list[_Job]:
        with self._lock:
            return [j for j in self._jobs if not j._cancelled]

    @property
    def job_count(self) -> int:
        with self._lock:
            return sum(1 for j in self._jobs if not j._cancelled)

    def get_metrics(self, name: str) -> dict[str, Any] | None:
        with self._lock:
            for j in self._jobs:
                if j.name == name:
                    return j.metrics.snapshot()
        return None

    def all_metrics(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return {j.name: j.metrics.snapshot() for j in self._jobs if not j._cancelled}

    # ── Execution ─────────────────────────────────────────────────────

    def _make_run_event(self, job: _Job) -> threading.Event:
        """Create a per-invocation cancel event, registered with the job.

        Each concurrent instance gets its own event so timeouts don't
        cross-cancel other instances.  job.cancel() signals ALL active events.
        The event is automatically unregistered when the invocation finishes.
        """
        evt = threading.Event()
        # Inherit cancellation if job was already cancelled
        if job._cancelled:
            evt.set()
        with job._events_lock:
            job._active_events.append(evt)
        return evt

    def _release_run_event(self, job: _Job, evt: threading.Event) -> None:
        with job._events_lock:
            try:
                job._active_events.remove(evt)
            except ValueError:
                pass

    def _invoke_fn(self, job: _Job, run_event: threading.Event) -> Any:
        """Invoke job function (sync path). Handles cancel_event injection,
        async coroutines via a persistent event loop, and timeouts.

        For async jobs: dispatched to a shared background event loop via
        asyncio.run_coroutine_threadsafe().  This avoids creating/destroying
        a loop per invocation (the old asyncio.run() approach), so coroutines
        can share resources like connection pools across calls.
        For sync jobs: uses _run_with_timeout with the per-invocation run_event.

        Security: only ``cancel_event`` is injected, only if the function
        explicitly declares it as a parameter (POSITIONAL_OR_KEYWORD or
        KEYWORD_ONLY). The injected value is always a threading.Event.
        """
        kw: dict[str, Any] = {}
        if job._wants_cancel:
            kw["cancel_event"] = run_event

        if job._is_async:
            loop = self._ensure_async_loop()
            coro = job.fn(**kw)
            if job.timeout > 0:
                async def _guarded() -> Any:
                    try:
                        return await asyncio.wait_for(asyncio.ensure_future(coro), timeout=job.timeout)
                    except asyncio.TimeoutError:
                        raise _JobTimeoutError(f"Timed out after {job.timeout}s")
                future = asyncio.run_coroutine_threadsafe(_guarded(), loop)
            else:
                future = asyncio.run_coroutine_threadsafe(coro, loop)
            return future.result()

        # Sync job
        if job.timeout > 0:
            fn = (lambda: job.fn(**kw)) if kw else job.fn
            return _run_with_timeout(fn, job.timeout, run_event)
        return job.fn(**kw)

    async def _invoke_fn_async(self, job: _Job, run_event: threading.Event) -> Any:
        """Invoke job function (async-native path). Coroutines run directly
        with await; sync functions are offloaded to the thread executor.

        asyncio.wait_for gives real cancellation for coroutines.
        """
        kw: dict[str, Any] = {}
        if job._wants_cancel:
            kw["cancel_event"] = run_event

        if job._is_async:
            coro = job.fn(**kw)
            if job.timeout > 0:
                try:
                    return await asyncio.wait_for(asyncio.ensure_future(coro), timeout=job.timeout)
                except asyncio.TimeoutError:
                    raise _JobTimeoutError(f"Timed out after {job.timeout}s")
            return await coro

        # Sync function in async context: offload to executor
        loop = asyncio.get_running_loop()
        fn = (lambda: job.fn(**kw)) if kw else job.fn
        if job.timeout > 0:
            return await loop.run_in_executor(
                None, lambda: _run_with_timeout(fn, job.timeout, run_event),
            )
        return await loop.run_in_executor(None, fn)

    def _handle_success(self, job: _Job, val: Any, duration: float) -> None:
        job.metrics.record_success(duration)
        job._consecutive_timeouts = 0
        if job.on_success:
            try: job.on_success(job.name, val, duration)
            except Exception:
                logger.exception("on_success hook failed for job '%s'", job.name)

    def _handle_timeout(self, job: _Job, duration: float, run_event: threading.Event | None = None) -> _JobTimeoutError:
        job.metrics.record_timeout(duration)
        job._consecutive_timeouts += 1
        if run_event is not None:
            run_event.set()
        logger.warning("Job '%s' timed out after %.1fs", job.name, duration)
        # Circuit breaker: auto-pause after N consecutive timeouts
        if job.max_consecutive_timeouts > 0 and job._consecutive_timeouts >= job.max_consecutive_timeouts:
            job.paused = True
            logger.error(
                "Job '%s' auto-paused: %d consecutive timeouts (limit=%d). "
                "Resume manually after investigating the root cause.",
                job.name, job._consecutive_timeouts, job.max_consecutive_timeouts,
            )
        if job.on_timeout:
            try: job.on_timeout(job.name, duration)
            except Exception:
                logger.exception("on_timeout hook failed for job '%s'", job.name)
        return _JobTimeoutError(f"Timed out after {job.timeout}s")

    def _handle_failure(self, job: _Job, e: Exception, duration: float) -> None:
        job.metrics.record_failure(duration, str(e))
        job._consecutive_timeouts = 0
        if job.on_failure:
            try: job.on_failure(job.name, e, duration)
            except Exception:
                logger.exception("on_failure hook failed for job '%s'", job.name)
        if self._on_error:
            try: self._on_error(job.name, e)
            except Exception: pass
        else:
            logger.error("Job '%s' failed: %s", job.name, e, exc_info=True)

    def _run_job_with_retries(self, job: _Job) -> Any:
        """Execute job (sync) with retry, timeout, metrics, and lifecycle hooks.

        WARNING: retry delays use time.sleep(), which blocks the calling
        thread.  When running in a ThreadPoolExecutor with max_workers=N,
        a job with retries=R and retry_delay=D (backoff=B) can block a
        worker for up to D*(B^R - 1)/(B - 1) seconds on sleeps alone.
        Example: retries=3, delay=2s, backoff=2 → up to 14s blocked.
        The async path (_run_job_with_retries_async) uses asyncio.sleep()
        and does NOT have this problem.
        """
        last_exc: Exception | None = None
        attempts = 1 + job.retries
        delay = job.retry_delay
        run_event = self._make_run_event(job)

        try:
            for attempt in range(attempts):
                if attempt > 0:
                    job.metrics.record_retry()
                    time.sleep(delay)
                    delay *= job.retry_backoff

                t0 = time.monotonic()
                try:
                    val = self._invoke_fn(job, run_event)
                    self._handle_success(job, val, time.monotonic() - t0)
                    return val
                except _JobTimeoutError:
                    last_exc = self._handle_timeout(job, time.monotonic() - t0, run_event)
                except Exception as e:
                    duration = time.monotonic() - t0
                    last_exc = e
                    if attempt == attempts - 1:
                        self._handle_failure(job, e, duration)

            raise last_exc  # type: ignore[misc]
        finally:
            self._release_run_event(job, run_event)

    async def _run_job_with_retries_async(self, job: _Job) -> Any:
        """Execute job (async-native) with retry, timeout, metrics, and hooks.

        Uses asyncio.sleep for retry delays (non-blocking).
        Uses asyncio.wait_for for timeouts (real cancellation).
        """
        last_exc: Exception | None = None
        attempts = 1 + job.retries
        delay = job.retry_delay
        run_event = self._make_run_event(job)

        try:
            for attempt in range(attempts):
                if attempt > 0:
                    job.metrics.record_retry()
                    await asyncio.sleep(delay)
                    delay *= job.retry_backoff

                t0 = time.monotonic()
                try:
                    val = await self._invoke_fn_async(job, run_event)
                    self._handle_success(job, val, time.monotonic() - t0)
                    return val
                except _JobTimeoutError:
                    last_exc = self._handle_timeout(job, time.monotonic() - t0, run_event)
                except Exception as e:
                    duration = time.monotonic() - t0
                    last_exc = e
                    if attempt == attempts - 1:
                        self._handle_failure(job, e, duration)

            raise last_exc  # type: ignore[misc]
        finally:
            self._release_run_event(job, run_event)

    def _finalize_job(self, job: _Job) -> None:
        """Common post-execution cleanup: release instance, reschedule or cancel."""
        job.release_instance(update_stats=True)
        if job.one_shot:
            job.cancel()
            if self._store:
                self._store.delete(job.name)
        else:
            job.reschedule()
            self._persist_job(job)

    def _execute_and_finalize(self, job: _Job) -> Any:
        if job._cancelled:
            job.release_instance()
            return None
        try:
            return self._run_job_with_retries(job)
        finally:
            self._finalize_job(job)

    async def _execute_and_finalize_async(self, job: _Job) -> Any:
        """Async-native execution with finalization."""
        if job._cancelled:
            job.release_instance()
            return None
        try:
            return await self._run_job_with_retries_async(job)
        finally:
            self._finalize_job(job)

    def _execute_sync(self, job: _Job) -> JobResult:
        result = JobResult()
        try:
            val = self._execute_and_finalize(job)
            result._set_sync(value=val)
        except Exception as e:
            result._set_sync(exception=e)
        return result

    def _dispatch_jobs(self) -> list[tuple[_Job, int]]:
        """Snapshot due jobs and acquire ALL needed instances atomically.

        Returns (job, acquired_count) pairs.  Both the due-check and the
        instance acquisition happen under self._lock, eliminating the
        race between the old _dispatch_jobs + _acquire_extra split.
        """
        dispatch: list[tuple[_Job, int]] = []
        with self._lock:
            for j in self._jobs:
                if not j.is_due:
                    continue
                runs_needed = 1
                if j.missed_policy == MissedPolicy.RUN_ALL:
                    runs_needed = max(1, j.missed_intervals() + 1)
                acquired = 0
                for _ in range(runs_needed):
                    if j.acquire_instance():
                        acquired += 1
                    else:
                        break
                if acquired == 0:
                    continue
                skipped = runs_needed - acquired
                if skipped > 0:
                    logger.warning(
                        "Job '%s' RUN_ALL: %d of %d missed intervals skipped "
                        "(max_instances=%d reached)",
                        j.name, skipped, runs_needed, j.max_instances,
                    )
                dispatch.append((j, acquired))
        return dispatch

    def _cleanup_cancelled(self) -> None:
        with self._lock:
            self._jobs = [j for j in self._jobs if not (j._cancelled and not j.is_running)]

    def tick_once(self) -> list[JobResult]:
        results: list[JobResult] = []
        dispatch = self._dispatch_jobs()
        pool = self._ensure_pool()

        for job, total in dispatch:
            for _ in range(total):
                if pool:
                    fut = pool.submit(self._execute_and_finalize, job)
                    results.append(JobResult(fut))
                else:
                    results.append(self._execute_sync(job))

        self._cleanup_cancelled()
        return results

    async def tick_once_async(self) -> list[Any]:
        """Async-native tick: dispatches coroutine jobs with asyncio.create_task
        and sync jobs via run_in_executor. Returns completed results.

        This is the async counterpart of tick_once(). Coroutine jobs get real
        cancellation via asyncio.wait_for — no zombie threads.
        """
        dispatch = self._dispatch_jobs()
        if not dispatch:
            self._cleanup_cancelled()
            return []

        loop = asyncio.get_running_loop()
        tasks: list[asyncio.Task | asyncio.Future] = []

        for job, total in dispatch:
            for _ in range(total):
                if job._is_async:
                    # Native async dispatch — real cancellation on timeout
                    task = asyncio.create_task(
                        self._execute_and_finalize_async(job),
                        name=f"reloj:{job.name}",
                    )
                    tasks.append(task)
                else:
                    # Sync job in async context — offload to thread
                    fut = loop.run_in_executor(None, self._execute_and_finalize, job)
                    tasks.append(fut)

        # Gather all, don't cancel on first failure
        results = await asyncio.gather(*tasks, return_exceptions=True)
        self._cleanup_cancelled()
        return list(results)

    def run(self) -> None:
        self._running = True
        self._ensure_pool()
        logger.info("Scheduler started (%d jobs, tick=%.2fs)", len(self.pending), self._tick)
        try:
            while self._running:
                self.tick_once()
                time.sleep(self._tick)
        except KeyboardInterrupt:
            logger.info("Scheduler interrupted")
        finally:
            self._running = False
            self._cleanup_resources()
            logger.info("Scheduler stopped")

    def stop(self) -> None:
        self._running = False

    async def run_async(self) -> None:
        """Async-native scheduler loop. Coroutine jobs run with real
        asyncio.wait_for cancellation — no abandoned threads.

        Sync jobs are offloaded to the default executor.
        """
        self._running = True
        try:
            while self._running:
                await self.tick_once_async()
                await asyncio.sleep(self._tick)
        except asyncio.CancelledError:
            pass
        finally:
            self._running = False
            self._persist_all()

    def __repr__(self) -> str:
        return f"Scheduler(jobs={self.job_count}, running={self._running})"

