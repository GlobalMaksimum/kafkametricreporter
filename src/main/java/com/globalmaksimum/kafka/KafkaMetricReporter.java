/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
/*
 * Copyright 2018 Global Maksimum Data & Information Technologies. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.globalmaksimum.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.KafkaThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class KafkaMetricReporter implements MetricsReporter, Configurable, Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaMetricReporter.class);
    private static final Set<String> metricNames;


    public static class GBMMetricsReporterConfig extends AbstractConfig {
        private static final ConfigDef CONFIG;
        private static final Set<String> CONFIGS = new HashSet<>();
        public static final String PREFIX = "gbm.metrics.reporter.";
        // Two unique configurations
        public static final String GBM_METRICS_TOPIC_CONFIG = "gbm.metrics.topic";
        public static final String GBM_METRICS_REPORTING_INTERVAL_MS_CONFIG = PREFIX + "metrics.reporting.interval.ms";
        // Default values
        public static final String DEFAULT_GBM_METRICS_TOPIC = "__GBMMetrics";
        private static final long DEFAULT_GBM_METRICS_REPORTING_INTERVAL_MS = 60000;
        private static final String PRODUCER_ID = "GBMMetricsReporter";

        public GBMMetricsReporterConfig(Map<?, ?> originals, boolean doLog) {
            super(CONFIG, originals, doLog);
        }

        static {
            ProducerConfig.configNames().forEach(name -> CONFIGS.add(PREFIX + name));
            CONFIG = new ConfigDef().define(PREFIX + CommonClientConfigs.CLIENT_ID_CONFIG,
                    ConfigDef.Type.STRING,
                    PRODUCER_ID,
                    ConfigDef.Importance.LOW,
                    "The _producer id for GBM metrics reporter")
                    .define(GBM_METRICS_TOPIC_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_GBM_METRICS_TOPIC,
                            ConfigDef.Importance.HIGH,
                            "")
                    .define(GBM_METRICS_REPORTING_INTERVAL_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            DEFAULT_GBM_METRICS_REPORTING_INTERVAL_MS,
                            ConfigDef.Importance.HIGH,
                            "");
        }

        public static String config(String baseConfigName) {
            String configName = PREFIX + baseConfigName;
            if (!CONFIGS.contains(configName)) {
                throw new IllegalArgumentException("The base config name " + baseConfigName + " is not defined.");
            }
            return configName;
        }

        static Properties parseProducerConfigs(Map<String, ?> configMap) {
            Properties props = new Properties();
            for (Map.Entry<String, ?> entry : configMap.entrySet()) {
                if (entry.getKey().startsWith(PREFIX)) {
                    props.put(entry.getKey().replace(PREFIX, ""), entry.getValue());
                }
            }
            return props;
        }
    }


    static {
        metricNames = new HashSet<>();
        metricNames.add("request-latency-avg");
        metricNames.add("request-latency-max");
        metricNames.add("request-rate");
        metricNames.add("response-rate");
        metricNames.add("incoming-byte-rate");
        metricNames.add("outgoing-byte-rate");
        metricNames.add("connection-count");
        metricNames.add("connection-creation-rate");
        metricNames.add("connection-close-rate");
        metricNames.add("io-ratio");
        metricNames.add("io-time-ns-avg");
        metricNames.add("io-wait-ratio");
        metricNames.add("select-rate");
        metricNames.add("io-wait-time-ns-avg");
        metricNames.add("request-size-max");
        metricNames.add("request-size-avg");
        metricNames.add("byte-rate");
        metricNames.add("record-send-rate");
        metricNames.add("compression-rate");
        metricNames.add("record-retry-rate");
        metricNames.add("record-error-rate");
        metricNames.add("records-lag-max");
        metricNames.add("fetch-size-avg");
        metricNames.add("fetch-size-max");
        metricNames.add("bytes-consumed-rate");
        metricNames.add("records-per-request-avg");
        metricNames.add("records-consumed-rate");
        metricNames.add("fetch-rate");
        metricNames.add("fetch-latency-avg");
        metricNames.add("fetch-latency-max");
        metricNames.add("fetch-throttle-time-avg");
        metricNames.add("fetch-throttle-time-max");
        metricNames.add("assigned-partitions");
        metricNames.add("commit-latency-avg");
        metricNames.add("commit-latency-max");
        metricNames.add("commit-rate");
        metricNames.add("join-rate");
        metricNames.add("join-time-avg");
        metricNames.add("join-time-max");
        metricNames.add("sync-rate");
        metricNames.add("sync-time-avg");
        metricNames.add("sync-time-max");
        metricNames.add("heartbeat-rate");
        metricNames.add("heartbeat-response-time-max");
        metricNames.add("last-heartbeat-seconds-ago");

    }

    private HashMap<MetricName, Metric> metricMap = new HashMap<>();
    private KafkaProducer _producer;
    private long _lastReportingTime;
    private long _reportingIntervalMs;
    private int _numMetricSendFailure;
    private String _gbmMetricsTopic;
    private boolean _shutdown;

    private static final int MAX_FRACTION_DIGITS = 340;
    private static final ThreadLocal<NumberFormat> NUMBER_FORMATTER =
            ThreadLocal.withInitial(() -> {
                NumberFormat numberFormat = NumberFormat.getInstance(Locale.ENGLISH);
                numberFormat.setMaximumFractionDigits(MAX_FRACTION_DIGITS);
                numberFormat.setGroupingUsed(false);
                numberFormat.setMinimumFractionDigits(1);
                return numberFormat;
            });

    @Override
    public void init(List<KafkaMetric> metrics) {

        synchronized (metricMap) {
            metrics.forEach(m -> metricMap.put(m.metricName(), m));
        }
        KafkaThread _metricsReporterRunner = new KafkaThread("GBMMetricsReporterRunner", this, true);
        _metricsReporterRunner.start();
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        synchronized (metricMap) {
            metricMap.put(metric.metricName(), metric);
        }
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        synchronized (metricMap) {
            metricMap.remove(metric.metricName());
        }
    }

    @Override
    public void close() {
        LOG.info("Closing GBM metrics reporter.");
        _shutdown = true;
        if (_producer != null) {
            _producer.close(5, TimeUnit.SECONDS);
        }
    }

    @Override
    public void run() {
        LOG.info("Starting GBM metrics reporter with reporting interval of {} ms.", _reportingIntervalMs);
        try {
            while (!_shutdown) {
                long now = System.currentTimeMillis();
                LOG.debug("Reporting metrics for time {}.", now);
                try {
                    if (now > _lastReportingTime + _reportingIntervalMs) {
                        _numMetricSendFailure = 0;
                        _lastReportingTime = now;
                        reportMetrics(now);
                    }
                    try {
                        _producer.flush();
                    } catch (InterruptException ie) {
                        if (_shutdown) {
                            LOG.info("GBM metric reporter is interrupted during flush due to shutdown request.");
                        } else {
                            throw ie;
                        }
                    }
                } catch (Exception e) {
                    LOG.error("Got exception in GBM metrics reporter", e);
                }
                // Log failures if there is any.
                if (_numMetricSendFailure > 0) {
                    LOG.warn("Failed to send {} metrics for time {}", _numMetricSendFailure, now);
                }
                _numMetricSendFailure = 0;
                long nextReportTime = now + _reportingIntervalMs;
                LOG.debug("Reporting finished for time {} in {} ms. Next reporting time {}",
                        now, System.currentTimeMillis() - now, nextReportTime);
                while (!_shutdown && now < nextReportTime) {
                    try {
                        Thread.sleep(nextReportTime - now);
                    } catch (InterruptedException ie) {
                        // let it go
                    }
                    now = System.currentTimeMillis();
                }
            }
        } finally {
            LOG.info("GBM metrics reporter exited.");
        }
    }


    private void reportMetrics(long now) {
        synchronized (metricMap) {
            Map<String, List<String>> values = new HashMap<>();
            metricMap.forEach((k, v) -> {
                if (metricNames.contains(k.name()) && !(
                        v.metricValue().equals(Double.NaN)
                                || v.metricValue().equals(Double.NEGATIVE_INFINITY)
                                || v.metricValue().equals(Double.POSITIVE_INFINITY))
                ) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("kafka_client_");
                    sb.append(k.group());
                    k.tags().forEach((k1, v1) -> sb.append("," + k1 + "=" + v1));
                    String key = sb.toString();
                    String attribute = k.name() + "=" + NUMBER_FORMATTER.get().format(v.metricValue());
                    if (values.containsKey(key)) {
                        values.get(key).add(attribute);
                    } else {
                        List<String> attributes = new ArrayList<>();
                        attributes.add(attribute);
                        values.put(key, attributes);
                    }
                }
            });
            values.forEach((k, v) -> {
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(_gbmMetricsTopic, k + " " + String.join(",", v) + " " + now);
                _producer.send(producerRecord, (recordMetadata, e) -> {
                    if (e != null) {
                        LOG.warn("Failed to send GBM metric {}", k);
                        _numMetricSendFailure++;
                    }
                });
            });
        }
    }


    @Override
    public void configure(Map<String, ?> configs) {
        Properties producerProps = GBMMetricsReporterConfig.parseProducerConfigs(configs);
        if (!producerProps.containsKey(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)) {
            Object port = configs.get("port");
            String bootstrapServers = "localhost:" + (port == null ? "9092" : String.valueOf(port));
            producerProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            LOG.info("Using default value of {} for {}", bootstrapServers,
                    GBMMetricsReporterConfig.config(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
        }
        if (!producerProps.containsKey(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)) {
            String securityProtocol = "PLAINTEXT";
            producerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
            LOG.info("Using default value of {} for {}", securityProtocol,
                    GBMMetricsReporterConfig.config(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        }
        GBMMetricsReporterConfig reporterConfig = new GBMMetricsReporterConfig(configs, false);

        setIfAbsent(producerProps,
                ProducerConfig.CLIENT_ID_CONFIG,
                reporterConfig.getString(GBMMetricsReporterConfig.config(CommonClientConfigs.CLIENT_ID_CONFIG)));
        // Set batch.size and linger.ms to a big number to have better batching.
        setIfAbsent(producerProps, ProducerConfig.LINGER_MS_CONFIG, "30000");
        setIfAbsent(producerProps, ProducerConfig.BATCH_SIZE_CONFIG, "800000");
        setIfAbsent(producerProps, ProducerConfig.RETRIES_CONFIG, "5");
        setIfAbsent(producerProps, ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        setIfAbsent(producerProps, ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        setIfAbsent(producerProps, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        setIfAbsent(producerProps, ProducerConfig.ACKS_CONFIG, "all");
        producerProps.list(System.out);
        _producer = new KafkaProducer<>(producerProps);
        _gbmMetricsTopic = reporterConfig.getString(GBMMetricsReporterConfig.GBM_METRICS_TOPIC_CONFIG);
        _reportingIntervalMs = reporterConfig.getLong(GBMMetricsReporterConfig.GBM_METRICS_REPORTING_INTERVAL_MS_CONFIG);
    }

    private void setIfAbsent(Properties props, String key, String value) {
        if (!props.containsKey(key)) {
            props.setProperty(key, value);
        }
    }
}
