#!/bin/bash
# Activate the uv-managed .venv and keep the shell interactive
source .venv/bin/activate
exec "$SHELL"
