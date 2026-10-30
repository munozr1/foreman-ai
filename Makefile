.PHONY: up down spike demo-race demo-guarded demo-serial demo-race-llm test test-unit test-integration test-restart check

up:
	docker compose up --build --wait --wait-timeout 180

down:
	docker compose down

spike: up
	docker compose run --rm app python scripts/spike.py

demo-race: up
	docker compose run --rm app python -m demo.scripted_race

demo-guarded: up
	docker compose run --rm app python -m demo.scripted_race --guarded

demo-serial: up
	docker compose run --rm app python -m demo.scripted_race --serial

demo-race-llm: up
	docker compose run --rm app python -m demo.llm_race

test: up
	docker compose run --rm app python -m pytest -q -s
	python3 scripts/test_restart.py

test-unit:
	PYTHONPATH=src:. .venv/bin/python -m pytest -q -m 'not integration'

test-integration: up
	docker compose run --rm app python -m pytest -q -s -m integration

test-restart: up
	python3 scripts/test_restart.py

check:
	docker compose run --rm app foremen check --run-id "$(RUN_ID)" --wait 30
