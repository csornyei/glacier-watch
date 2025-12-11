FROM ghcr.io/csornyei/glacier-watch-base:latest

COPY . . 
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked

ENTRYPOINT [ "uv", "run" ]
CMD ["python", "-m", "src.process.main", "--dry-run"]