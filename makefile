
discover:
	uv run python -m src.discover.main --project_id=kebnekaise --date_from=2020-01-01 --date_to=2025-10-31

download:
	uv run python -m src.download.main

process:
	uv run python -m src.process.main

process-dr:
	uv run python -m src.process.main --dry-run

dem:
	uv run python -m src.dem.main --project_id=kebnekaise

db-init:
	uv run python -m src.utils.db