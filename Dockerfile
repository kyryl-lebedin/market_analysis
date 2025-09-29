FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN pip install poetry
RUN poetry config virtualenvs.create false

# Copy Poetry files
COPY pyproject.toml poetry.lock ./
RUN poetry install --only=main --no-root

# Copy source code
COPY src/ ./src/
COPY README.md ./

# Create necessary directories
RUN mkdir -p data/bronze data/silver data/gold logs



# Default command
CMD ["python", "-m", "job_pipeline.cli", "--help"]