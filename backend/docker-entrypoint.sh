#!/bin/sh
set -e
echo "Applying database migrations..."
alembic upgrade head
echo "Starting API on :8000"
exec uvicorn app.main:create_app --factory --host 0.0.0.0 --port 8000 --proxy-headers --timeout-graceful-shutdown 15
