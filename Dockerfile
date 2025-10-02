# Build stage
FROM python:3.12-slim as builder
WORKDIR /app
COPY pyproject.toml poetry.lock ./
RUN pip install poetry poetry-plugin-export && \
    poetry export --only=main --format=requirements.txt --output=requirements.txt

# Runtime stage
FROM python:3.12-slim
WORKDIR /app

# Install dependencies directly
COPY --from=builder /app/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/

# Set environment
ENV PYTHONPATH="/app/src"

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash app && \
    chown -R app:app /app
USER app

# Set entrypoint
ENTRYPOINT ["python", "-m", "job_pipeline.cli"]
CMD ["--help"]