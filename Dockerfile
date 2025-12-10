# Dockerfile - reproducible environment for the assignment
FROM python:3.11-slim

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy project files
COPY . /app

# Install Python deps
RUN pip install --no-cache-dir -r requirements.txt

# Make runner executable
RUN chmod +x /app/run_all.sh

ENTRYPOINT ["/app/run_all.sh"]