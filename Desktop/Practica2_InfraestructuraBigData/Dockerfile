FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && \
    apt-get install -y procps gcc g++ make curl liblz4-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# Set working directory
WORKDIR /scripts

# Keep container running
CMD ["tail", "-f", "/dev/null"]