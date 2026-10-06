# syntax=docker/dockerfile:1.7
# One image, three control-plane roles (api | relay | consumer) plus the worker simulator.
# Targets:  runtime (default, production deps only)  |  dev (adds test & quality tooling)

FROM python:3.12-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app/src
WORKDIR /app
RUN groupadd --system app && useradd --system --gid app --home-dir /app app

FROM base AS runtime
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY alembic.ini ./
COPY alembic ./alembic
COPY src ./src
USER app
EXPOSE 8000
ENTRYPOINT ["python", "-m"]
CMD ["control_plane", "api"]

FROM runtime AS dev
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends make \
 && rm -rf /var/lib/apt/lists/*
COPY requirements-dev.txt .
RUN pip install -r requirements-dev.txt
COPY pyproject.toml Makefile ./
COPY tests ./tests
COPY scripts ./scripts
RUN chown -R app:app /app
USER app
ENTRYPOINT []
CMD ["make", "_check-in-container"]
