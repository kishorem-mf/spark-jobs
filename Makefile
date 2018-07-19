.PHONY: dockerlint
dockerlint:
	docker run --rm -i -v ${PWD}/.hadolint.yml:/.hadolint.yaml hadolint/hadolint:v1.10.3 < docker/airflow/Dockerfile
