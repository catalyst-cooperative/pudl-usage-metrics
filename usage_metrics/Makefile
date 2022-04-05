build:
	docker compose build

shell:
	docker compose run --rm app /bin/bash

sql_shell:
	docker compose run --rm postgres bash -c 'psql -U $$POSTGRES_USER -h $$POSTGRES_HOST'

launch:
	docker compose up

run_elt:
	docker compose run --rm app /bin/bash -c 'dagster job execute -m usage_metrics -r usage_metrics'

unit_test:
	docker compose run --rm app pytest usage_metrics_tests/unit
