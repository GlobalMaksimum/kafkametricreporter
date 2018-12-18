Kafka metric reporter for producers and consumers. It takes metrics and writes to a kafka topic with influxdb line format
Some of the code has been copied from https://github.com/linkedin/cruise-control 

Example usage with a producer. Just add below snippet properties to your producer. KafkaMetricProducer will start a new producer and
write influx line protocol formatted data into DEFAULT_GBM_METRICS_TOPIC with 60000 milliseconds interval.
```java
props.put("metric.reporters", "com.globalmaksimum.kafka.KafkaMetricReporter");
props.put(GBMMetricsReporterConfig.GBM_METRICS_REPORTING_INTERVAL_MS_CONFIG, "10000");
props.put(GBMMetricsReporterConfig.config(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), "quickstart.confluent.io:19094, quickstart.confluent.io:29094, quickstart.confluent.io:39094");
props.put(GBMMetricsReporterConfig.config(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG), SecurityProtocol.SASL_PLAINTEXT.name);
props.put(GBMMetricsReporterConfig.config("sasl.kerberos.service.name"), "kafka");
```
