.PHONY: pycodestyle
pycodestyle:
	pycodestyle --show-source .

.PHONY: lint
lint:
	bash ./scripts/pylint.sh --rcfile=.pylintrc ./ohub
#	./dags was excluded for the moment because of ton of errors

.PHONY: pytest
pytest:
	pytest --cov-config=.coveragerc --cov=ohub .

.PHONY: clean
clean:
	find . -name '__pycache__' | xargs rm -rf

.PHONY: local-ci
local-ci: | clean pycodestyle lint pytest
