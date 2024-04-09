#!/bin/sh
source .venv/bin/activate
python -m flask --app src/main run -p $PORT --debug