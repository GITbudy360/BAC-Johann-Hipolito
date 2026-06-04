# Use the official lightweight Python image
FROM python:3.11-slim

# Prevent Python from writing .pyc files to disk and ensure console output is not buffered
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set the working directory
WORKDIR /app

# Create a non-root user for security purposes
RUN addgroup --system appgroup && adduser --system --group appuser

# Copy only the requirements first to leverage Docker cache
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY main.py .

# Transfer ownership of the application files to the non-root user
RUN chown -R appuser:appgroup /app

# Switch to the non-root user
USER appuser

# Document the port the container will listen on
EXPOSE 8000

# Start the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]