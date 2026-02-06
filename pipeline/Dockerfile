FROM python:3.13.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /code
ENV PATH="/code/.venv/bin:$PATH"
COPY  pyproject.toml .python-version uv.lock ./



 
# Add virtual environment to PATH so we can use installed packages


# Copy dependency files first (better layer caching)
COPY "pyproject.toml" "uv.lock" ".python-version" ./
# Install dependencies from lock file (ensures reproducible builds)
RUN uv sync --locked

# Copy application code
COPY pipeline.py pipeline.py 
COPY ingest_data.py ingest_data.py

# Set entry point
ENTRYPOINT ["python", "pipeline.py"]