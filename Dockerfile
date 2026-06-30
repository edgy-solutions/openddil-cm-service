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
# cli/submit_cm_event.py is the operator-facing helper used by the
# customer-bundle's k8s/seed-cm-events.sh demo-iteration script (and by
# operators paste-and-edit one-liners for manual CmEvent injection).
# Bake it into the image at /app/cli so `kubectl exec deploy/cm-service
# -- python /app/cli/submit_cm_event.py ...` works without bind-mounting
# from a checkout. Same pattern as bootstrap above: small Python entry
# script the runtime container uses with a different command override.
# Without this, the seeder fails with "python: can't open file
# '/app/cli/submit_cm_event.py': [Errno 2] No such file or directory"
# (observed 2026-06-29 during demo prep).
COPY cli /app/cli

EXPOSE 8090/tcp

CMD ["python", "/app/src/main.py"]
