.PHONY: up down logs test lint fmt

up:      ## build and start api + mongo + redis
	docker compose up --build -d

down:    ## stop everything and remove volumes
	docker compose down -v

logs:
	docker compose logs -f api

test:    ## run the full test suite inside compose (real MongoDB + Redis)
	docker compose --profile test run --rm --build tests

lint:
	ruff check app tests && ruff format --check app tests

fmt:
	ruff check --fix app tests && ruff format app tests
