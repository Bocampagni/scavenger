.PHONY: help install format run

help:
	@echo "Available commands:"
	@echo "  make setup          Install and set up the development environment"
	@echo "  make format         Format the codebase using ruff"
	@echo "  make run            Run the main application"

setup:
	uv sync

format:
	uv run ruff format .
	uv run ruff check --fix .
	uv run ruff check --select I --fix .

run:
	uv run python main.py

.DEFAULT_GOAL := help
