#!/bin/bash
set -e

echo "Installing tox ..."
pip3 install -q tox

# Running tests
echo "Running tests ..."
cd python
tox -q -e starrocks
