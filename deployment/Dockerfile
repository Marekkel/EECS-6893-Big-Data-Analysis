# Base image with Java for PySpark
FROM eclipse-temurin:11-jre-jammy

# Install Python
RUN apt-get update && apt-get install -y \
    python3.10 \
    python3-pip \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY backend/requirements.txt .

# Install Python dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy backend code
COPY backend/app_gcp.py .
COPY backend/predictor_gcp.py .

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Port for Cloud Run
ENV PORT=8080
EXPOSE 8080

# Copy startup script
COPY backend/startup.py .

# Start the application with uvicorn directly
CMD exec python3 -m uvicorn app_gcp:app --host 0.0.0.0 --port 8080 --log-level info
