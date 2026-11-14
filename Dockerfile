# syntax=docker/dockerfile:1
FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_ROOT_USER_ACTION=ignore \
    PIP_DEFAULT_TIMEOUT=60 \
    PIP_RETRIES=5 \
    PYTHONPATH=/srv

WORKDIR /srv

RUN groupadd --system app && useradd --system --gid app --home /srv app

# ---- runtime dependencies --------------------------------------------------
# Dependencies are installed from pyproject.toml alone (against an empty package stub that is then
# uninstalled), so editing application code never invalidates this layer. The app itself runs from
# source via PYTHONPATH. The cache mount keeps downloaded wheels across builds without baking them
# into the image.
FROM base AS deps
COPY pyproject.toml ./
RUN mkdir app && touch app/__init__.py
RUN --mount=type=cache,target=/root/.cache/pip pip install . && pip uninstall -y cohort-insights-api

# ---- runtime image ---------------------------------------------------------
FROM deps AS runtime
COPY app ./app
USER app
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]

# ---- test image (adds test deps + tests) -----------------------------------
FROM deps AS test
RUN --mount=type=cache,target=/root/.cache/pip pip install ".[test]" && pip uninstall -y cohort-insights-api
COPY app ./app
COPY tests ./tests
USER app
CMD ["pytest", "-q", "-p", "no:cacheprovider"]
