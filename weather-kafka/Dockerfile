FROM python:3.9-slim

# Install dependencies
RUN pip install kafka-python flask requests

# Copy application code
COPY weather_kafka_combined.py /app/weather_kafka_combined.py

# Expose Flask port
EXPOSE 5001

# Run the Flask app
CMD ["python", "/app/weather_kafka_combined.py"]
