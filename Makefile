CODE := landuse_api

run:
	poetry run launch_api

format:
	poetry run isort $(CODE)
	poetry run black $(CODE)

lint:
	poetry run pylint $(CODE)