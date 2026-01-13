FROM python:3.12-slim

WORKDIR /app

# System dependencies 
RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install --no-cache-dir uv

# Copy dependency files first 
COPY pyproject.toml uv.lock ./

# Install deps into a local venv (.venv)
RUN uv sync --frozen

# Make sure installed CLIs (dagster/streamlit/etc) are on PATH
ENV PATH="/app/.venv/bin:$PATH"

# Copy project files
COPY . .

# Create persistent-ish directories (mounts in compose will override these)
RUN mkdir -p /app/duckdb /app/dbt/target /app/dagster_home

# Env
ENV PYTHONPATH=/app
ENV DAGSTER_HOME=/app/dagster_home
ENV DUCKDB_PATH=/app/duckdb/football.duckdb

# Ports: Dagster UI + Streamlit
EXPOSE 3000 8501

# Default CMD: Dagster 
CMD ["dagster", "dev", "-m", "dagster_defs", "--host", "0.0.0.0", "--port", "3000"]
