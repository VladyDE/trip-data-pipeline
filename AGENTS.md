# Repository Guidelines

## Project Structure & Module Organization

- `src/Tproject/` contains the installable Python package and shared helpers.
- `src/Tproject_etl/` contains Bronze/Silver ingestion and transformation code; put reusable Spark logic in `utilities/` and layer-specific work in `transformations/`.
- `gold/` holds the SQL definitions for Gold-layer Delta tables.
- `resources/` contains Databricks Asset Bundle job, pipeline, trigger, and app resource YAML.
- `apps/dashboard/` is the Streamlit application, with its runtime requirements and app configuration.
- `tests/` contains pytest tests and shared Spark fixtures in `conftest.py`. Test data belongs in `fixtures/`; reference outputs belong in `gold/` or `resources/` as appropriate.

## Build, Test, and Development Commands

Run commands from the repository root:

```bash
uv run pytest                 # run the Spark and SQL test suite
uv run ruff check .           # lint Python code
uv build --wheel              # build the wheel used by the bundle
databricks bundle deploy --target dev  # deploy development resources
```

Use `databricks auth login` before deployment. Validate bundle YAML with `databricks bundle validate --target dev` before deploying configuration changes.

## Coding Style & Naming Conventions

Target Python 3.10–3.12. Follow Ruff with a 120-character line limit, four-space indentation, snake_case functions and variables, and PascalCase classes. Add type hints to public helpers where practical. Keep Spark transformations deterministic and return DataFrames rather than collecting data in production code. Use lowercase, underscore-separated column names (for example, `rating_viaje`), preserving the project’s column-sanitization convention. Name resource files by purpose, such as `Tproject_etl.pipeline.yml` and `SDP_trigger.job.yml`.

## Testing Guidelines

Use pytest and the shared `spark` fixture. Name files `test_*.py` and test functions `test_<behavior>`. Cover expected values, nulls, invalid input, and schema/column changes. Test Gold SQL through `spark.sql()` as the existing Gold tests do. Run `uv run pytest` and `uv run ruff check .` before opening a pull request; no formal coverage threshold is configured.

## Commit & Pull Request Guidelines

Recent history uses short, imperative summaries such as `Fix dashboard screenshot link` and `Update README with project goals and description`. Keep commits focused and describe the affected area. Pull requests should explain the pipeline, SQL, bundle, or dashboard change; link the related issue when available; include test results; and attach dashboard screenshots for visible UI changes. Call out catalog, schema, volume, or warehouse configuration changes explicitly—do not commit secrets or credentials.
