# Dockerfile for cdr-generator
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY cdr_generator.py .

# Run the app
CMD ["python", "-u", "cdr-generator.py"]
