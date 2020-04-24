# RADAR-base S3 connector
This repository contains S3-Connector of RADAR-base platform

## Direct usage

1. In addition to Zookeeper, Kafka-broker(s), Schema-registry and Rest-proxy, minio should be running.
2. Load the `radar-s3-connector-*.jar` to CLASSPATH

    ```shell
    export CLASSPATH=/path/to/radar-s3-connector-*.jar
    ```
      
3. Configure S3 Connector properties.

    ```ini
      name=radar-s3-sink-connector
      connector.class=io.confluent.connect.s3.S3SinkConnector
      tasks.max=1
      topics=test,test1,test2
      
      s3.bucket.name=bucketname
      flush.size=10
      
      aws.access.key.id=accesskey # this did not work with minio config, although adding environment variables to docker-container works
      aws.secret.access.key=secretkey
      
      store.url=http://localhost:9000/
      storage.class=io.confluent.connect.s3.storage.S3Storage
      format.class=org.radarbase.connect.s3.RadarBaseAvroFormat
    ```
   
4. Run the connector. To run the connector in `standalone mode` (on an environment confluent platform is installed)
   
    ```shell
    connect-standalone /etc/schema-registry/connect-avro-standalone.properties path-to-your-s3.properties
    ```

## Docker usage

To run this connector as a docker container, use the [radarbase/radar-s3-connector](https://hub.docker.org/radarbase/radar-s3-connector) docker image. 
It runs the []Confluent Kafka Connect Storage Cloud 5.4.1]([here](https://docs.confluent.io/current/connect/kafka-connect-s3/index.html) using a custom record write provider to store both keys and values.

Create the docker image:
```
$ docker build -t radarbase/radar-s3-connector ./
```

Or pull from dockerhub:
```
$ docker pull radarbase/radar-s3-connector:1.0.0
```

### Configuration

This image has to be extended with a volume with appropriate `sink-s3.properties`, for example:

```ini
      name=radar-s3-sink-connector
      connector.class=io.confluent.connect.s3.S3SinkConnector
      tasks.max=1
      topics=test,test1,test2
      
      s3.bucket.name=bucketname
      flush.size=10
      
      aws.access.key.id=accesskey # this did not work with minio config, although adding environment variables to docker-container works
      aws.secret.access.key=secretkey
      
      store.url=http://localhost:9000/
      storage.class=io.confluent.connect.s3.storage.S3Storage
      format.class=org.radarbase.connect.s3.RadarBaseAvroFormat
```

The docker-compose service could be defined as follows:

```yaml
  #---------------------------------------------------------------------------#
  # RADAR S3 connector                                                     #
  #---------------------------------------------------------------------------#
  radar-s3-connector:
    image: radarbase/radar-s3-connector:1.0.0
    restart: on-failure
    volumes:
      - ./sink-s3.properties:/etc/kafka-connect/sink-s3.properties
    depends_on:
      - zookeeper-1
      - kafka-1
      - kafka-2
      - kafka-3
      - schema-registry-1
      - minio
      - mc
    environment:
      CONNECT_BOOTSTRAP_SERVERS: PLAINTEXT://kafka-1:9092,PLAINTEXT://kafka-2:9092,PLAINTEXT://kafka-3:9092
      CONNECT_REST_PORT: 8083
      CONNECT_GROUP_ID: "default"
      CONNECT_CONFIG_STORAGE_TOPIC: "default.config"
      CONNECT_OFFSET_STORAGE_TOPIC: "default.offsets"
      CONNECT_STATUS_STORAGE_TOPIC: "default.status"
      CONNECT_KEY_CONVERTER: "io.confluent.connect.avro.AvroConverter"
      CONNECT_VALUE_CONVERTER: "io.confluent.connect.avro.AvroConverter"
      CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL: "http://schema-registry-1:8081"
      CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL: "http://schema-registry-1:8081"
      CONNECT_INTERNAL_KEY_CONVERTER: "org.apache.kafka.connect.json.JsonConverter"
      CONNECT_INTERNAL_VALUE_CONVERTER: "org.apache.kafka.connect.json.JsonConverter"
      CONNECT_OFFSET_STORAGE_FILE_FILENAME: "/tmp/connect2.offset"
      CONNECT_REST_ADVERTISED_HOST_NAME: "radar-s3-connector"
      CONNECT_ZOOKEEPER_CONNECT: zookeeper-1:2181
      CONNECTOR_PROPERTY_FILE_PREFIX: "sink-s3"
      KAFKA_HEAP_OPTS: "-Xms256m -Xmx768m"
      KAFKA_BROKERS: 3
      CONNECT_LOG4J_LOGGERS: "org.reflections=ERROR"
      AWS_ACCESS_KEY: "minioaccesskey"
      AWS_SECRET_KEY: "miniosecretkey"
```

## Contributing

Code should be formatted using the [Google Java Code Style Guide](https://google.github.io/styleguide/javaguide.html).
If you want to contribute a feature or fix browse our [issues](https://github.com/RADAR-base/radar-s3-connector/issues), and please make a pull request.
