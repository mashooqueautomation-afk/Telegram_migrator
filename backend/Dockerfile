FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
RUN chmod +x docker-entrypoint.sh \
 && useradd --system --uid 10001 --no-create-home appuser
USER appuser

EXPOSE 8000
ENTRYPOINT ["./docker-entrypoint.sh"]
