#!/bin/bash
# Helper script to run dbt commands with environment variables loaded

# Load environment variables from .env file
if [ -f "../.env" ]; then
    export $(grep -v '^#' ../.env | xargs)
else
    echo "Error: ../.env file not found"
    exit 1
fi

# Run dbt with the provided command
dbt "$@"

