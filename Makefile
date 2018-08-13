.PHONY: dockerlint
dockerlint:
	docker run --rm -i -v ${PWD}/.hadolint.yml:/.hadolint.yaml hadolint/hadolint:v1.10.3 < docker/airflow/Dockerfile

.PHONY: lint
lint:
	bash ./scripts/pylint.sh --rcfile=.pylintrc --output-format=colorized ./ohub
#	./dags was excluded for the moment because of ton of errors
