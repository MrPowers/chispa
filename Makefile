.PHONY: install
install: ## Install the Poetry environment
	@echo "Creating virtual environment using Poetry"
	@poetry install

.PHONY: check
check: ## Run code quality checks
	@echo "Running pre-commit hooks"
	@poetry run pre-commit run -a
	@poetry run mypy chispa

.PHONY: test
test: ## Run unit tests
	@echo "Running unit tests"
	@poetry run pytest tests --cov=chispa --cov-report=term

.PHONY: test-cov-html
test-cov-html: ## Run unit tests and create a coverage report
	@echo "Running unit tests and generating HTML report"
	@poetry run pytest tests --cov=chispa --cov-report=html

.PHONY: test-cov-xml
test-cov-xml: ## Run unit tests and create a coverage report in xml format
	@echo "Running unit tests and generating XML report"
	@poetry run pytest tests --cov=chispa --cov-report=xml

.PHONY: build
build: clean-build ## Build wheel and sdist files using Poetry
	@echo "Creating wheel and sdist files"
	@poetry build

.PHONY: clean-build
clean-build: ## clean build artifacts
	@rm -rf dist

.PHONY: publish
publish: ## Publish a release to PyPI
	@echo "Publishing: Dry run."
	@poetry config pypi-token.pypi $(PYPI_TOKEN)
	@poetry publish --dry-run
	@echo "Publishing."
	@poetry publish

.PHONY: build-and-publish
build-and-publish: build publish ## Build and publish

.PHONY: docs-test
docs-test: ## Test if documentation can be built without warnings or errors
	@poetry run mkdocs build -s

.PHONY: docs
docs: ## Build and serve the documentation
	@poetry run mkdocs serve

# Inspired by https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.PHONY: help
help: ## Show help for the commands
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
