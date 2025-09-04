FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir \
    requests>=2.28.0 \
    requests-oauthlib>=1.3.0 \
    boto3

# Create app directory
WORKDIR /app

# Copy application files
COPY fetch_likers.py /app/fetch_likers.py
COPY requirements.txt /app/requirements.txt

# Create data directory
RUN mkdir -p /data

# Set permissions
RUN chmod +x /app/fetch_likers.py

# Create volume for persistent data
VOLUME ["/data"]

# Set default environment variables
ENV DB_PATH=/data/state.db
ENV OUT_DIR=/data
ENV EXPORT_EVERY_SECS=300

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sqlite3; conn = sqlite3.connect('/data/state.db'); conn.close()" || exit 1

# Run the application
CMD ["python", "/app/fetch_likers.py"]
