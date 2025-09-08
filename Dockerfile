FROM bitnami/spark:latest

USER root

RUN apt-get update && \
	apt-get install -y curl && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt python-dotenv

COPY src/ src/
COPY .env .

# Download PostgreSQL JDBC driver into Spark jars directory
RUN curl -o /opt/bitnami/spark/jars/postgresql-42.5.0.jar \
	https://jdbc.postgresql.org/download/postgresql-42.5.0.jar && \
	chmod 644 /opt/bitnami/spark/jars/postgresql-42.5.0.jar
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

ENTRYPOINT ["docker-entrypoint.sh"]
