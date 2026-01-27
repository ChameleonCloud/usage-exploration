#!/usr/bin/env bash
ruff check --fix --output-format concise .
ruff format .
