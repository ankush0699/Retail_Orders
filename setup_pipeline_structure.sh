#!/bin/bash

# Create pipeline directory and files
mkdir -p pipeline

# Create empty Python module files
touch pipeline/__init__.py
touch pipeline/download.py
touch pipeline/extract.py
touch pipeline/transform.py
touch pipeline/load.py
touch pipeline/config.py

echo "Empty pipeline folder and module files created."
