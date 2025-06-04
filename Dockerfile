# Uses an official Bitnami base image that already includes Spark and Python
FROM bitnami/spark:3.5.0

# Sets the working directory inside the container
WORKDIR /app

# Temporarily switches to the root user to install packages
USER root

# Attempts to clean the apt cache and create missing directories
# before installing wget.
RUN rm -rf /var/lib/apt/lists/* \
    && mkdir -p /var/lib/apt/lists/partial \
    && apt-get clean \
    && apt-get update && apt-get install -y wget

# Defines the SQLite JDBC driver version
ENV SQLITE_JDBC_VERSION 3.45.1.0

# Downloads the SQLite JDBC driver directly and places it in the Spark jars folder
RUN wget -P /opt/bitnami/spark/jars/ https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/${SQLITE_JDBC_VERSION}/sqlite-jdbc-${SQLITE_JDBC_VERSION}.jar

# Switches back to the default Bitnami image user (ID 1001 is common for Bitnami)
# This is important for maintaining security and compatibility with the base image
USER 1001

# Copies the requirements file into the container
COPY requirements.txt .
COPY scripts/bz_layer.py .
COPY scripts/sv_layer.py .

# Installs the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copies the PySpark script into the container
COPY main_flow.py .
