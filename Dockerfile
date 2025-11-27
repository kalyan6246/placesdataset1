FROM python:3.10-slim
 
# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
&& rm -rf /var/lib/apt/lists/*
 
# Set working directory
WORKDIR /app
 
# Copy code
COPY . .
 
# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt
 
# EXPOSE port for Cloud Run HTTP server
EXPOSE 8080
 
# ENTRYPOINT - this runs your Flask API
ENTRYPOINT ["python", "main.py"]
