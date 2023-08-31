#! /usr/bin/env sh
# Example WSGI Server Configuration as Gunicorn w/ Uvicorn Worker for Production

set -e

# Path to Gunicorn config file
GUNICORN_CONF=/gunicorn_conf.py

# Path to WSGI module
MODULE_NAME=api

# FastAPI instance name
APP_NAME=app

# Set Gunicorn Worker Class
export WORKER_CLASS=${WORKER_CLASS:-"uvicorn.workers.UvicornWorker"}

# Set App Module
APP_MODULE=$MODULE_NAME:$APP_NAME

# Start Gunicorn with WSGI Application w/ Uvicorn Worker and Gunicorn Config
exec gunicorn -k "$WORKER_CLASS" -c "$GUNICORN_CONF" "$APP_MODULE"