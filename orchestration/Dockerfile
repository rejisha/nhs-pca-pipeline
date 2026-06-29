FROM apache/airflow:2.8.1-python3.10

# Switch to root to install system-level packages
USER root

# Install Java (required by PySpark) and curl/gnupg (required to add Microsoft's repo)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        default-jre-headless \
        curl \
        gnupg \
        unixodbc-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Add Microsoft's package repository and install ODBC Driver 18 for SQL Server
RUN curl https://packages.microsoft.com/keys/microsoft.asc | tee /etc/apt/trusted.gpg.d/microsoft.asc && \
    curl https://packages.microsoft.com/config/debian/12/prod.list | tee /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# JAVA_HOME for PySpark
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

# Switch back to airflow user for Python package installs
USER airflow

# Copy and install project's Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt