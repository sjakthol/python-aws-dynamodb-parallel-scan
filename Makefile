## Build targets
.PHONY: lint test format lint-pylint lint-black lint-mypy lint-bandit
test:
	uv run pytest -vv --log-level=DEBUG --cov aws_dynamodb_parallel_scan --cov-report term-missing

lint: lint-ruff-check lint-ruff-format lint-mypy lint-bandit
lint-ruff-check:
	uv run ruff check
lint-ruff-format:
	uv run ruff format --check
lint-mypy:
	uv run mypy src/aws_dynamodb_parallel_scan tests
lint-bandit:
	uv run bandit -q -r src/aws_dynamodb_parallel_scan

format:
	uv run ruff format
	uv run ruff check --fix