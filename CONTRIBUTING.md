# Contributing to reloj

## Ground rules

1. **One file, zero deps.** All logic stays in `reloj.py`. No external dependencies. Ever.
2. **Tests in `tests/`.** pytest only. No sleep-based timing where avoidable — use `_force_due()` and manual `tick_once()`.
3. **Don't add features that push toward APScheduler territory.** If someone needs distributed locking, job serialization, or a REST API, they need a different tool.

## Setup

```bash
git clone https://github.com/ElEscribanoSilente/reloj.git
cd reloj
pip install pytest
python -m pytest tests/ -v
```

No virtual environment ceremony needed — there are no dependencies.

## Making changes

1. Fork the repo and create a branch from `main`.
2. Write tests for your change.
3. Run `python -m pytest tests/ -v` and ensure all tests pass.
4. Keep your PR focused. One fix per PR, one feature per PR.

## What we accept

- Bug fixes with a reproduction case
- Performance improvements with benchmarks
- Documentation fixes
- Test improvements (especially reducing timing sensitivity)

## What we don't accept

- New external dependencies
- Splitting into multiple files / packages
- Features that aren't useful without additional infrastructure
- "Modernization" PRs that rewrite working code for style points

## Releasing

Maintainers only. Bump version in `reloj.py` and `pyproject.toml`, update `CHANGELOG.md`, create a GitHub release. PyPI publish is automated via GitHub Actions.
