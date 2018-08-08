.PHONY: lint
lint:
	find ./ohub -name '*.py' | grep -v vendor | xargs bash ./scripts/pylint.sh --rcfile=.pylintrc
#	./dags was excluded for the moment because of ton of errors
