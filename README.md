## kafka-test-util
This will help developer and qa to test messaging service queue by consuming and publishing data

### Configuration for PLAIN mechanism
Path for configuration file: [application.properties](src/main/resources/application.properties)
```properties
bootstrap.servers=
group.id=
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="" password="";
ssl.truststore.location=src/resources/ssl.jks
ssl.truststore.password=
key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
auto.offset.reset=latest
```

### topic
- `topic=topic-name`

### poll-timeout-millis
- `poll-timeout-millis=1000`

### auto.offset.reset
- `auto.offset.reset=earliest`
- `auto.offset.reset=latest`