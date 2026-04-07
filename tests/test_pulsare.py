"""Tests for pulsare — pytest-based, deterministic where possible.

Uses manual tick_once() calls instead of sleep-based timing loops to
avoid flaky failures under CI load.  Sleep is only used where thread
concurrency genuinely requires wall-clock time (e.g. max_instances).
"""

import asyncio
import copy
import os
import tempfile
import threading
import time

import pytest

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pulsare import (
    Scheduler, _Job, JobMetrics, SQLiteStore,
    every, cron, once, MissedPolicy, _CronSpec,
)


# ─── Helpers ─────────────────────────────────────────────────────────────────


def _force_due(job):
    """Make a job immediately due."""
    job.next_run = 0


# ─── 1. Basic scheduling ────────────────────────────────────────────────────


class TestBasicScheduling:
    def test_basic_job_runs(self):
        s = Scheduler(tick=0.1)
        counter = {"n": 0}
        j = s.add(lambda: counter.__setitem__("n", counter["n"] + 1),
                  every(1).seconds, name="basic")
        _force_due(j)
        s.tick_once()
        assert counter["n"] == 1

    def test_oneshot_runs_once(self):
        s = Scheduler()
        results = []
        s.add(lambda: results.append(1), once(after=0), name="oneshot")
        s.tick_once()
        s.tick_once()
        assert len(results) == 1

    def test_job_decorator(self):
        s = Scheduler()
        counter = {"n": 0}

        @s.job(once(after=0), name="decorated")
        def my_task():
            counter["n"] += 1

        s.tick_once()
        assert counter["n"] == 1


# ─── 2. Retry with backoff ──────────────────────────────────────────────────


class TestRetry:
    def test_retries_until_success(self):
        s = Scheduler()
        attempts = {"n": 0}

        def flaky():
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise RuntimeError("not yet")
            return "ok"

        j = s.add(flaky, once(after=0), name="flaky",
                  retries=3, retry_delay=0.001, retry_backoff=1.0)
        s.tick_once()
        m = j.metrics.snapshot()
        assert attempts["n"] == 3
        assert m["retry_count"] == 2
        assert m["success_count"] == 1

    def test_exhausted_retries_fail(self):
        s = Scheduler()

        def always_fail():
            raise RuntimeError("boom")

        j = s.add(always_fail, once(after=0), name="fail",
                  retries=2, retry_delay=0.001, retry_backoff=1.0)
        s.tick_once()
        m = j.metrics.snapshot()
        assert m["fail_count"] == 1
        assert m["retry_count"] == 2
        assert m["success_count"] == 0


# ─── 3. Timeout ─────────────────────────────────────────────────────────────


class TestTimeout:
    def test_timeout_fires(self):
        s = Scheduler()
        to_hits = []
        j = s.add(lambda: time.sleep(5), once(after=0), name="slow",
                  timeout=0.3,
                  on_timeout=lambda n, d: to_hits.append(d))
        try:
            s.tick_once()
        except Exception:
            pass
        assert len(to_hits) == 1
        assert j.metrics.snapshot()["timeout_count"] == 1

    def test_circuit_breaker_auto_pauses(self):
        s = Scheduler()
        j = s.add(lambda: time.sleep(5), every(1).seconds, name="breaker",
                  timeout=0.3, max_consecutive_timeouts=2)
        _force_due(j)
        try:
            s.tick_once()
        except Exception:
            pass
        assert j._consecutive_timeouts == 1
        assert not j.paused

        _force_due(j)
        try:
            s.tick_once()
        except Exception:
            pass
        assert j._consecutive_timeouts == 2
        assert j.paused

    def test_consecutive_timeouts_reset_on_success(self):
        s = Scheduler()
        call_count = {"n": 0}

        def sometimes_slow():
            call_count["n"] += 1
            if call_count["n"] <= 1:
                time.sleep(5)
            return "ok"

        j = s.add(sometimes_slow, every(1).seconds, name="resetter",
                  timeout=0.3, max_consecutive_timeouts=3)
        _force_due(j)
        try:
            s.tick_once()
        except Exception:
            pass
        assert j._consecutive_timeouts == 1

        _force_due(j)
        s.tick_once()
        assert j._consecutive_timeouts == 0

    def test_failure_resets_consecutive_timeouts(self):
        s = Scheduler()
        call_count = {"n": 0}

        def timeout_then_fail():
            call_count["n"] += 1
            if call_count["n"] == 1:
                time.sleep(5)  # will timeout
            raise RuntimeError("normal fail")

        j = s.add(timeout_then_fail, every(1).seconds, name="fail_reset",
                  timeout=0.3, max_consecutive_timeouts=5)
        # First call: timeout
        _force_due(j)
        try:
            s.tick_once()
        except Exception:
            pass
        assert j._consecutive_timeouts == 1

        # Second call: normal failure (should reset counter)
        _force_due(j)
        s.tick_once()
        assert j._consecutive_timeouts == 0


# ─── 4. Max instances ───────────────────────────────────────────────────────


class TestMaxInstances:
    def test_respects_max_instances(self):
        s = Scheduler(max_workers=4)
        peak = {"max": 0, "cur": 0}
        lock = threading.Lock()
        barrier = threading.Event()

        def track():
            with lock:
                peak["cur"] += 1
                peak["max"] = max(peak["max"], peak["cur"])
            barrier.wait(timeout=2)
            with lock:
                peak["cur"] -= 1

        j = s.add(track, every(1).seconds, name="limited", max_instances=2)
        _force_due(j)
        s.tick_once()  # dispatches up to max_instances
        _force_due(j)
        s.tick_once()  # should be blocked by max_instances
        time.sleep(0.5)  # let threads start
        assert peak["max"] <= 2
        barrier.set()  # release all threads
        time.sleep(0.5)  # let threads finish
        if s._pool:
            s._pool.shutdown(wait=True)


# ─── 5. Lifecycle hooks ─────────────────────────────────────────────────────


class TestLifecycleHooks:
    def test_on_success(self):
        s = Scheduler()
        results = []
        s.add(lambda: 42, once(after=0), name="hk_ok",
              on_success=lambda n, v, d: results.append(v))
        s.tick_once()
        assert results == [42]

    def test_on_failure(self):
        s = Scheduler()
        errors = []

        def bad():
            raise ValueError("boom")

        s.add(bad, once(after=0), name="hk_fail",
              on_failure=lambda n, e, d: errors.append(str(e)))
        s.tick_once()
        assert len(errors) == 1
        assert "boom" in errors[0]


# ─── 6. Metrics ─────────────────────────────────────────────────────────────


class TestMetrics:
    def test_success_and_fail_counts(self):
        s = Scheduler()
        cn = {"n": 0}

        def mfn():
            cn["n"] += 1
            if cn["n"] == 2:
                raise RuntimeError("fail")
            return cn["n"]

        j = s.add(mfn, every(1).seconds, name="mtest")
        for _ in range(3):
            _force_due(j)
            s.tick_once()
        m = j.metrics.snapshot()
        assert m["success_count"] == 2
        assert m["fail_count"] == 1
        assert m["avg_duration"] >= 0

    def test_all_metrics(self):
        s = Scheduler()
        j = s.add(lambda: None, every(1).seconds, name="m1")
        _force_due(j)
        s.tick_once()
        am = s.all_metrics()
        assert "m1" in am
        assert am["m1"]["success_count"] == 1


# ─── 7. Edge cases / validation ─────────────────────────────────────────────


class TestEdgeCases:
    def test_impossible_cron_rejected(self):
        with pytest.raises(ValueError, match="can never match"):
            cron("0 0 31 2 *")

    def test_ambiguous_day_rejected(self):
        with pytest.raises(ValueError, match="ambiguous"):
            every(3).day

    def test_max_instances_zero_rejected(self):
        with pytest.raises(ValueError, match="max_instances"):
            _Job(fn=lambda: None, spec=once(after=0), max_instances=0)

    def test_negative_interval_rejected(self):
        with pytest.raises(ValueError):
            every(0)

    def test_negative_once_rejected(self):
        with pytest.raises(ValueError):
            once(after=-1)


# ─── 8. Threaded JobResult ──────────────────────────────────────────────────


class TestJobResult:
    def test_threaded_result(self):
        with Scheduler(max_workers=2) as s:
            s.add(lambda: 99, once(after=0), name="ret")
            r = s.tick_once()
            assert r[0].result(timeout=2) == 99


# ─── 9. Name deduplication ──────────────────────────────────────────────────


class TestNameDedup:
    def test_duplicate_name_raises(self):
        s = Scheduler()
        s.add(lambda: None, once(after=0), name="foo")
        with pytest.raises(ValueError, match="already exists"):
            s.add(lambda: None, once(after=0), name="foo")

    def test_cancelled_name_can_be_reused(self):
        s = Scheduler()
        j = s.add(lambda: None, once(after=0), name="foo")
        j.cancel()
        # Should not raise — the old job is cancelled
        s.add(lambda: None, once(after=0), name="foo")

    def test_decorator_duplicate_raises(self):
        s = Scheduler()

        @s.job(once(after=0), name="bar")
        def task1():
            pass

        with pytest.raises(ValueError, match="already exists"):
            @s.job(once(after=0), name="bar")
            def task2():
                pass


# ─── 10. Job copy protection ────────────────────────────────────────────────


class TestJobCopyProtection:
    def test_copy_raises(self):
        j = _Job(fn=lambda: None, spec=once(after=0), name="copytest")
        with pytest.raises(TypeError, match="Cannot copy"):
            copy.copy(j)

    def test_deepcopy_raises(self):
        j = _Job(fn=lambda: None, spec=once(after=0), name="dcopytest")
        with pytest.raises(TypeError, match="Cannot deepcopy"):
            copy.deepcopy(j)


# ─── 11. Cron next_run ──────────────────────────────────────────────────────


class TestCronNextRun:
    def test_every_minute(self):
        c = cron("* * * * *")
        nr = c.next_run()
        assert nr > time.time()
        # Should be within ~61 seconds
        assert nr - time.time() < 62

    def test_specific_time(self):
        c = cron("30 14 * * *")
        nr = c.next_run()
        assert nr > time.time()

    def test_feb_29_finds_leap_year(self):
        c = cron("0 0 29 2 *")
        nr = c.next_run()
        assert nr > time.time()
        # Should find a Feb 29 within 4 years
        from datetime import datetime, timezone
        dt = datetime.fromtimestamp(nr, tz=timezone.utc)
        assert dt.month == 2
        assert dt.day == 29


# ─── 12. SQLiteStore UPSERT ─────────────────────────────────────────────────


class TestSQLiteStore:
    def test_save_and_load(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            path = f.name
        try:
            store = SQLiteStore(path)
            state = {
                "next_run": 1000.0, "last_run": 999.0, "run_count": 5,
                "paused": False, "success_count": 4, "fail_count": 1,
                "timeout_count": 0, "retry_count": 2,
                "total_duration": 10.0, "last_duration": 2.0,
                "last_error": None, "last_success_at": 998.0,
                "last_failure_at": 997.0,
            }
            store.save("test_job", state)
            loaded = store.load("test_job")
            assert loaded is not None
            assert loaded["next_run"] == 1000.0
            assert loaded["run_count"] == 5

            # Update via UPSERT
            state["run_count"] = 10
            store.save("test_job", state)
            loaded2 = store.load("test_job")
            assert loaded2["run_count"] == 10

            store.close()
        finally:
            os.unlink(path)

    def test_delete(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            path = f.name
        try:
            store = SQLiteStore(path)
            state = {
                "next_run": 1000.0, "last_run": 0.0, "run_count": 0,
                "paused": False, "success_count": 0, "fail_count": 0,
                "timeout_count": 0, "retry_count": 0,
                "total_duration": 0.0, "last_duration": 0.0,
                "last_error": None, "last_success_at": 0.0,
                "last_failure_at": 0.0,
            }
            store.save("to_delete", state)
            store.delete("to_delete")
            assert store.load("to_delete") is None
            store.close()
        finally:
            os.unlink(path)


# ─── 13. Async jobs (sync path) ─────────────────────────────────────────────


class TestAsyncJobs:
    def test_async_job_in_sync_scheduler(self):
        s = Scheduler()
        results = []

        async def async_task():
            results.append("done")
            return 42

        j = s.add(async_task, once(after=0), name="async_sync")
        s.tick_once()
        assert results == ["done"]
        s._shutdown_async_loop()

    def test_async_job_shares_loop(self):
        """Two async jobs dispatched from sync path should share the same loop."""
        s = Scheduler()
        loops = []

        async def capture_loop():
            loops.append(id(asyncio.get_running_loop()))

        s.add(capture_loop, once(after=0), name="loop1")
        s.tick_once()
        # Add another and tick
        s.add(capture_loop, once(after=0), name="loop2")
        s.tick_once()
        assert len(loops) == 2
        assert loops[0] == loops[1]  # same loop
        s._shutdown_async_loop()


# ─── 14. Pause / Resume / Cancel ────────────────────────────────────────────


class TestControl:
    def test_pause_resume(self):
        s = Scheduler()
        counter = {"n": 0}
        j = s.add(lambda: counter.__setitem__("n", counter["n"] + 1),
                  every(1).seconds, name="ctrl", tags={"grp"})
        _force_due(j)
        s.pause("grp")
        s.tick_once()
        assert counter["n"] == 0  # paused, should not run

        s.resume("grp")
        _force_due(j)
        s.tick_once()
        assert counter["n"] == 1

    def test_cancel_by_name(self):
        s = Scheduler()
        j = s.add(lambda: None, every(1).seconds, name="to_cancel")
        assert s.cancel_by_name("to_cancel") is True
        assert j._cancelled is True

    def test_purge(self):
        s = Scheduler()
        j = s.add(lambda: None, every(1).seconds, name="to_purge")
        j.cancel()
        removed = s.purge()
        assert removed == 1
        assert s.job_count == 0


# ─── 15. Interval / Schedule specs ──────────────────────────────────────────


class TestScheduleSpecs:
    def test_every_seconds(self):
        spec = every(5).seconds
        assert spec.to_seconds() == 5

    def test_every_minutes(self):
        spec = every(2).minutes
        assert spec.to_seconds() == 120

    def test_every_hours(self):
        spec = every(1).hours
        assert spec.to_seconds() == 3600

    def test_every_day_at(self):
        spec = every().day.at("09:00")
        nr = spec.next_run()
        assert nr > time.time()

    def test_every_monday(self):
        spec = every().monday
        nr = spec.next_run()
        assert nr > time.time()

    def test_at_invalid_time(self):
        with pytest.raises(ValueError, match="Invalid time"):
            every().day.at("25:00")

    def test_at_on_seconds(self):
        with pytest.raises(ValueError, match="only valid with"):
            every(5).seconds.at("09:00")

    def test_cron_parse_step(self):
        c = cron("*/5 * * * *")
        assert 0 in c._minute
        assert 5 in c._minute
        assert 3 not in c._minute

    def test_cron_parse_range(self):
        c = cron("0 9-17 * * *")
        assert c._hour == set(range(9, 18))

    def test_cron_invalid_fields(self):
        with pytest.raises(ValueError, match="5 fields"):
            cron("* * *")


# ─── 16. Anchor-based reschedule (anti-drift) ───────────────────────────────


class TestAnchorReschedule:
    def test_no_drift_on_interval(self):
        """Reschedule should anchor from scheduled time, not wall clock."""
        s = Scheduler()
        j = s.add(lambda: None, every(10).seconds, name="drift_test")
        # Set next_run to now (making it due) — this is the anchor point
        anchor = time.time()
        j.next_run = anchor
        s.tick_once()
        # next_run should be ~anchor + 10, NOT ~now + 10
        expected = anchor + 10
        assert abs(j.next_run - expected) < 0.5

    def test_clamp_to_present(self):
        """If anchor is far in the past, clamp to now instead of scheduling
        a burst of past runs."""
        s = Scheduler()
        j = s.add(lambda: None, every(5).seconds, name="clamp_test")
        j.next_run = time.time() - 100  # far in the past
        _force_due(j)
        s.tick_once()
        # Should not schedule 95 seconds in the past
        assert j.next_run >= time.time()
