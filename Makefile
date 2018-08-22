<<<<<<< HEAD
.PHONY: pycodestyle
pycodestyle:
	pycodestyle --show-source .
=======
.PHONY: dockerlint
dockerlint:
	docker run --rm -i -v ${PWD}/.hadolint.yml:/.hadolint.yaml hadolint/hadolint:v1.10.3 < docker/airflow/Dockerfile
>>>>>>> master

.PHONY: lint
lint:
	find ./ohub -name '*.py' | grep -v vendor | xargs bash ./scripts/pylint.sh --rcfile=.pylintrc --output-format=colorized
#	./dags was excluded for the moment because of ton of errors

.PHONY: pytest
pytest:
	pytest --cov-config=.coveragerc --cov=ohub .

.PHONY: clean
clean:
	find . -name '__pycache__' | xargs rm -rf

.PHONY: black
black:
	(find . -name '*.py' | xargs black --check) || true

.PHONY: local-ci
local-ci: | clean black pycodestyle lint pytest
