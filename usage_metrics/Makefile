build:
	docker compose build

shell:
	docker compose run --rm app /bin/bash

sql_shell:
	docker compose run --rm postgres bash -c 'psql -U $$POSTGRES_USER -h $$POSTGRES_HOST'