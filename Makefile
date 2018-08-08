.PHONY: lint
lint:
	bash ./scripts/pylint.sh --rcfile=.pylintrc --output-format=colorized ./ohub
#	./dags was excluded for the moment because of ton of errors
