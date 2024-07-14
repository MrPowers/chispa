.PHONY: install
install: ## Install the Poetry environment.
	@echo "Creating virtual environment using Poetry"
	@poetry install

.PHONY: test
test: ## Run unit tests.
	@echo "Running unit tests"
	@poetry run pytest tests

.PHONY: build
build: clean-build ## Build wheel and sdist files using Poetry.
	@echo "Creating wheel and sdist files"
	@poetry build

.PHONY: clean-build
clean-build: ## clean build artifacts
	@rm -rf dist

.PHONY: publish
publish: ## Publish a release to PyPI.
	@echo "Publishing: Dry run."
	@poetry config pypi-token.pypi $(PYPI_TOKEN)
	@poetry publish --dry-run
	@echo "Publishing."
	@poetry publish

.PHONY: build-and-publish
build-and-publish: build publish ## Build and publish.

.PHONY: docs-test
docs-test: ## Test if documentation can be built without warnings or errors.
	@poetry run mkdocs build -s

.PHONY: docs
docs: ## Build and serve the documentation.
	@poetry run mkdocs serve

.PHONY: help
help: ## Show help for the commands.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
