
limit:=10
log_level:=INFO
discover:
	uv run python -m src.discover.main --project_id=jotunheimen --date_from=2025-01-01 --date_to=2025-10-31 --log_level=$(log_level) --limit=$(limit) --dry_run

download:
	uv run python -m src.download.main

process:
	uv run python -m src.process.main

process-dr:
	uv run python -m src.process.main --dry-run

dem:
	uv run python -m src.dem.main --project_id=jotunheimen --log_level=$(log_level)

db-init:
	uv run python -m src.utils.db