build-docs:
	cp README.md docs/index.md

docsserve: venv venv/bin/mkdocs
	. venv/bin/activate; mkdocs serve

test: venv venv/bin/mkdocs
	. venv/bin/activate; pytest

coverage:  ## Run tests with coverage
	coverage erase
	coverage run -m pytest
	coverage report -m
	coverage xml

clean:
	rm -rf dist
	find . -type f -name "*.DS_Store" -ls -delete
	find . | grep -E "(__pycache__|\.pyc|\.pyo)" | xargs rm -rf
	find . | grep -E ".pytest_cache" | xargs rm -rf
	find . | grep -E ".ipynb_checkpoints" | xargs rm -rf
	rm -f .coverage

style: venv venv/bin/mkdocs
	. venv/bin/activate; isort --profile black .
	. venv/bin/activate; ruff format .

push:
	git push && git push --tags

build:
	. venv/bin/activate; pip build --wheel

publish-test:
	$(style clean build)
	twine upload -r testpypi dist/*

publish-prod:
	$(style clean build)
	twine upload dist/*

venv:
	python3 -m venv venv
	. venv/bin/activate; pip install -e .

venv/bin/mkdocs:
	. venv/bin/activate; pip install -e .\[dev]

