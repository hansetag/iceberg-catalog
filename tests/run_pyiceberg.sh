#!/bin/bash
# This script is meant to run in  apache/spark:3.5.1-java17-python3 like docker images
set -e

# Use pip3 to install the required packages
export HOME=/opt/spark/work-dir
export PATH=$PATH:/opt/spark/bin:/opt/spark/work-dir/.local/bin

echo "Installing tox ..."
pip3 install -q tox

# Running tests
echo "Running tests ..."
cd python
tox -q -e pyiceberg
