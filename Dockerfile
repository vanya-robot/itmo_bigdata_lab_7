FROM bitnami/spark:4.0.0

USER root

RUN apt-get update && \
    apt-get install -y curl gnupg unzip && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt python-dotenv

COPY src/ src/
COPY datamart/datamartproject/target/scala-2.13/datamart-csv-fat.jar .
COPY .env .

# пример CSV
COPY src/sql/source_data.csv /app/source_data.csv

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

ENTRYPOINT ["docker-entrypoint.sh"]
