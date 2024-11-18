# Start from an official Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy the application files into the container
COPY . /app/

# Install required dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Run the application
CMD ["python", "/app/func.py"]
