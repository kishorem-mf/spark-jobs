.PHONY: dockerlint
dockerlint:
	docker run --rm -i -v ${PWD}/.hadolint.yml:/.hadolint.yaml hadolint/hadolint:v1.10.3 < docker/airflow/Dockerfile

.PHONY: lint
lint:
	find ./ohub -name '*.py' | grep -v vendor | xargs bash ./scripts/pylint.sh --rcfile=.pylintrc --output-format=colorized
#	./dags was excluded for the moment because of ton of errors
