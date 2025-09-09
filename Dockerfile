FROM bitnami/spark:3.5.1

USER root

RUN apt-get update && \
    apt-get install -y curl gnupg unzip && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV SCALA_VERSION=2.12.18
RUN curl -fsL https://downloads.typesafe.com/scala/$SCALA_VERSION/scala-$SCALA_VERSION.deb -o scala.deb && \
    apt-get install -y ./scala.deb && rm scala.deb

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt python-dotenv

COPY src/ src/
COPY target/ target/
COPY .env .

# JDBC драйвер PostgreSQL
RUN curl -o /opt/bitnami/spark/jars/postgresql-42.5.0.jar \
    https://jdbc.postgresql.org/download/postgresql-42.5.0.jar && \
    chmod 644 /opt/bitnami/spark/jars/postgresql-42.5.0.jar

# пример CSV
COPY src/sql/source_data.csv /app/source_data.csv

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# компиляция Scala ETL в JAR прямо в контейнере
RUN mkdir -p /app/classes && \
    scalac -J-Xmx2g -cp "/opt/bitnami/spark/jars/*:/app/target/*" -d /app/classes src/datamart/DataMart.scala && \
    jar cf /app/target/datamart.jar -C /app/classes .

RUN chown -R 1001:root /app
USER 1001

WORKDIR /app

ENTRYPOINT ["docker-entrypoint.sh"]
