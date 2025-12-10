FROM python:3.13-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gdal-bin \
    libgdal-dev \
    proj-bin \
    libproj-dev \
    libgeos-dev \
    libexpat1 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-install-project

COPY . . 
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked

ENTRYPOINT [ "uv", "run" ]
CMD ["python", "-m", "src.process.main", "--dry-run"]