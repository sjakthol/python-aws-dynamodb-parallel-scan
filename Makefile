## Build targets
.PHONY: lint test format lint-pylint lint-black lint-mypy lint-bandit
test:
	poetry run pytest -vv --log-level=DEBUG --cov aws_dynamodb_parallel_scan --cov-report term-missing

lint: lint-pylint lint-black lint-mypy lint-bandit
lint-pylint:
	poetry run pylint --max-line-length=120 --score=n aws_dynamodb_parallel_scan.py
lint-black:
	poetry run black --check aws_dynamodb_parallel_scan.py tests
lint-mypy:
	poetry run mypy aws_dynamodb_parallel_scan.py tests
lint-bandit:
	poetry run bandit -q -r aws_dynamodb_parallel_scan.py

format:
	poetry run black aws_dynamodb_parallel_scan.py tests