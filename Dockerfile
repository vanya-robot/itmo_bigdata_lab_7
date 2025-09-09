FROM bitnami/spark:latest

USER root

# Установим hadoop client и curl
RUN apt-get update && \
    apt-get install -y curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt python-dotenv

# код и модель
COPY src/ src/
COPY .env .

# JDBC драйвер
RUN curl -o /opt/bitnami/spark/jars/postgresql-42.5.0.jar \
    https://jdbc.postgresql.org/download/postgresql-42.5.0.jar && \
    chmod 644 /opt/bitnami/spark/jars/postgresql-42.5.0.jar

# пример CSV сразу в контейнер
COPY src/sql/source_data.csv /app/source_data.csv

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

ENTRYPOINT ["docker-entrypoint.sh"]
