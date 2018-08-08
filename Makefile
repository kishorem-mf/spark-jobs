.PHONY: lint
lint:
	bash ./scripts/pylint.sh --rcfile=.pylintrc ./ohub
#	./dags was excluded for the moment because of ton of errors

.PHONY: pytest
pytest:
	pytest tests/ -s

.PHONY: local-ci
local-ci: | lint pytest
