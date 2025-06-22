# Use a slim Python 3.10 image
FROM python:3.10-slim-bookworm

# Set the working directory in the container
WORKDIR /app

# Install OpenJDK 17
# Update package list and install openjdk-17-jdk
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable for PySpark
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV PATH="$PATH:$JAVA_HOME/bin"

# Install Python dependencies
COPY . .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the backend application code
#COPY ../backend/app.py .

# Expose the port your Flask app will run on
EXPOSE 5000

# Command to run the Flask application
# Use a production-ready WSGI server like Gunicorn in a real application
CMD ["python", "app.py"]