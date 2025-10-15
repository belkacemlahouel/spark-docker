# Base image with Java and Python
FROM openjdk:11-slim

# Set environment variables
ENV SPARK_VERSION=3.5.7 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=python3

# Install dependencies
RUN apt-get update && apt-get install -y \
    curl \
    python3 \
    python3-pip \
    python3-setuptools \
    vim \
    zsh \
    && apt-get clean

# Download and extract Spark
RUN curl -fsSL https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

# Add Spark binaries to PATH
ENV PATH=$SPARK_HOME/bin:$PATH

# Optional: Install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install -r /tmp/requirements.txt

# Set workdir
# Copy your app script into the image
COPY src/* /app/
WORKDIR /app

# Default command (interactive shell)
CMD ["spark-submit", "app/app.py"]

