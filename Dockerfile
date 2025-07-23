FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    libsnappy-dev \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire era-parser package
COPY . .

# Install the package in development mode
RUN pip install -e .

# Create output directory
RUN mkdir -p /app/output

# Only mount output directory
VOLUME ["/app/output"]

# Copy .env file if it exists (it will be imported by the Python code)
COPY .env* ./

# Set default command
ENTRYPOINT ["era-parser"]
CMD ["--help"]