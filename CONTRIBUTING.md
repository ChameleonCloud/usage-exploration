# Contributing

## Dev Setup

Preferred (`uv`):

```bash
uv sync --all-extras --dev
```

Alternative (`pip`):

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[all]'
pip install pre-commit pytest ruff ty
```

## Quality Checks

Run before opening a PR:

```bash
ruff check src tests
pytest
ty check src/chameleon_usage/extract
```

If you are changing pipeline or viz code, also run:

```bash
ty check src
```

## Project Structure

- `src/chameleon_usage/extract/`: database extraction and parquet dump.
- `src/chameleon_usage/ingest/`: raw table loading and interval shaping.
- `src/chameleon_usage/pipeline.py`: usage metric computation.
- `src/chameleon_usage/viz/`: plotting helpers.
- `src/chameleon_usage/cli.py`: command-line entrypoint.
- `etc/site.yml`: example site config.

## Config Contract

`site.yml` entries use:

- `site_name`
- `db_uri`
- `data_dir`

## Adding New Source Tables

When adding a new DB table to extraction:

1. Add it to `src/chameleon_usage/extract/dump_db.py` `TABLES`.
2. Keep `src/chameleon_usage/sources.py` registry in sync.
3. Add or update schema/ingest wiring as needed.
4. Add tests for behavior changes.

## PR Checklist

- Keep changes minimal and obvious.
- Update README examples if CLI/config behavior changes.
- Update tests for user-visible behavior changes.
- Do not introduce optional-dependency imports at module import time in extractor paths.
