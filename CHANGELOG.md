# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-04-06

### Added
- Fluent interval API: `every(5).seconds`, `every().monday.at("09:00")`
- Cron expressions (5-field standard)
- One-shot delayed tasks: `once(after=30)`
- Concurrent execution with configurable thread pool
- Per-job max concurrent instances
- Retry with exponential backoff
- Execution timeout with cooperative cancellation via `cancel_event`
- Circuit breaker: `max_consecutive_timeouts` auto-pauses hung jobs
- Missed job policies: skip, run_once, run_all
- Lifecycle hooks: on_success, on_failure, on_timeout
- Built-in metrics (duration, success/fail counts, last_error)
- Job tagging with pause/resume/cancel by tag
- Timezone-aware scheduling (system tz auto-detected)
- Async-native scheduler loop (`run_async()`)
- Persistent event loop for async jobs in sync context
- Pluggable persistence via `JobStore` protocol
- Built-in `SQLiteStore` (WAL mode, UPSERT, zero deps)
- Anchor-based interval reschedule (prevents cumulative drift)
- Job name uniqueness enforcement
- Context manager support
