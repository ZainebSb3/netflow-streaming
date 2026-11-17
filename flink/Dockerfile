FROM flink:1.19.1-scala_2.12-java11

# Connectors are not part of the Flink distribution: add Kafka, JDBC and the PostgreSQL driver.
ADD --chown=flink:flink https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar /opt/flink/lib/
ADD --chown=flink:flink https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar /opt/flink/lib/
ADD --chown=flink:flink https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/flink/lib/
