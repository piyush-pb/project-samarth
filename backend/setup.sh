#!/bin/bash
set -e

echo "Upgrading pip and build tools..."
pip install --upgrade pip setuptools wheel

echo "Installing requirements..."
pip install -r requirements.txt

echo "Installation complete!"

