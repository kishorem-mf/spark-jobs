.PHONY: pycodestyle
pycodestyle:
	pycodestyle --show-source .

.PHONY: lint
lint:
	bash ./scripts/pylint.sh --rcfile=.pylintrc ./ohub
#	./dags was excluded for the moment because of ton of errors

.PHONY: pytest
pytest:
	pytest --cov-config .coveragerc --cov=ohub/ tests/ -s

.PHONY: local-ci
local-ci: | pycodestyle lint pytest
