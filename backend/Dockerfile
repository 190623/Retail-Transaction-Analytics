FROM python:3.10-slim

# Install Java required by PySpark
RUN apt-get update && \
    apt-get install -y openjdk-21-jre-headless && \
    rm -rf /var/lib/apt/lists/*

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Set working directory
WORKDIR /app

# Copy dependencies first for Docker layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend application
COPY app.py .

# Create directory for uploaded CSV files
RUN mkdir -p /app/uploads

# Backend port
EXPOSE 5000

# Start Flask + PySpark backend
CMD ["python", "app.py"]