# Custom Airflow image for AirflowEtl project
FROM apache/airflow:2.9.3-python3.11

# ---- Install Java (needed for PySpark) ----
USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME so PySpark can find Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ---- Install extra Python dependencies as 'airflow' user ----
USER airflow

RUN pip install --no-cache-dir \
    google-api-python-client \
    google-auth-httplib2 \
    google-auth-oauthlib \
    minio \
    pyspark==3.5.1
