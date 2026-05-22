# =============================================================================
# openddil-cm-service — Configuration Management Faust App
# =============================================================================
# Two-stage build, same pattern as openddil-sensor-ingest:
#   1. builder  — installs deps into a venv via uv + pyproject.toml
#   2. runtime  — slim image that runs main.py
# =============================================================================

# ---------- Stage 1: Builder ----------
FROM python:3.11-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc g++ make librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv && uv venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /app
COPY pyproject.toml .
RUN uv pip compile pyproject.toml -o requirements.txt \
    && uv pip install --no-cache -r requirements.txt

# ---------- Stage 2: Runtime ----------
FROM python:3.11-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        librdkafka1 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1
# Generated proto stubs are mounted from openddil-contracts/gen/python
ENV PYTHONPATH=/proto:/app/src

WORKDIR /app
COPY src /app/src
# bootstrap/register_subscriptions.py is executed by the cm-service-bootstrap
# Helm hook Job (same image, command overridden) — bake it into the image.
COPY bootstrap /app/bootstrap

EXPOSE 8090/tcp

CMD ["python", "/app/src/main.py"]
