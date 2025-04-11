#!/bin/bash

# Create pipeline directory and files
mkdir -p test_pipeline

# Create empty Python module files
touch setup_test_pipeline/__init__.py
touch setup_test_pipeline/test_download.py
touch setup_test_pipeline/test_extract.py
touch setup_test_pipeline/test_transform.py
touch setup_test_pipeline/test_load.py
touch setup_test_pipeline/test_config.py

echo "Empty setup_test_pipeline folder and module files created."