#!/bin/bash
set -e
dbt seed && dbt snapshot && dbt run
