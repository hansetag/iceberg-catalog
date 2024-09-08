#!/bin/bash
# This script is meant to run in  apache/spark:3.5.1-java17-python3 like docker images
set -e

# Use pip3 to install the required packages
export HOME=/opt/spark/work-dir
export PATH=$PATH:/opt/spark/bin:/opt/spark/work-dir/.local/bin

echo "Installing tox ..."
pip3 install -q tox

echo "Modifying the PYTHONPATH ..."
# Add pyspark to the PYTHONPATH
# Iterate over all zips in $SPARK_HOME/python/lib and add them to the PYTHONPATH
for i in /opt/spark/python/lib/*.zip; do
    export PYTHONPATH=$PYTHONPATH:$i
done

# Running tests
echo "Running tests ..."
cd python
tox -q -e spark_remote_signing,spark_sts,spark_adls
