package com.echostreams.pulsar.jms.config;

import java.util.Properties;


public class PulsarConfigBuilder {
    private Properties config = new Properties();

    public Properties build() {
        return config;
    }

	// TODO load config for the given path for tls/authenz authentication

    /**
     * @return the brokerList
     *//*
    public String getBroker() {
		return (String) config.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
	}

	*//**
     * @param brokerList
     *            the brokerList to set
     *//*
	public PulsarConfigBuilder broker(String broker) {
		config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
		config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
		return this;
	}

	*//**
     * @return the serializerClass
     *//*
	public String getValueSerializerClass() {
		return (String) config
				.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
	}

	*//**
     * @param serializerClass
     *            the serializerClass to set
     *//*
	public PulsarConfigBuilder valueSerializerClass(String valueSerializerClass) {
		config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				valueSerializerClass);
		config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueSerializerClass);
		return this;
	}

	*//**
     * @return the keySerializerClass
     *//*
	public String getKeySerializerClass() {
		return (String) config.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
	}

	*//**
     * @param keySerializerClass
     *            the keySerializerClass to set
     *//*
	public PulsarConfigBuilder keySerializerClass(String keySerializerClass) {
		config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				keySerializerClass);
		config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keySerializerClass);
		return this;
	}

	public PulsarConfigBuilder keyDeserializerClass(String keyDeserializerClass) {
		config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass);
		return this;
	}

	public String getAckConfig() {
		return (String) config.get(ProducerConfig.ACKS_CONFIG);
	}

	public PulsarConfigBuilder ackConfig(String acks) {
		config.put(ProducerConfig.ACKS_CONFIG, acks);
		return this;
	}

	public PulsarConfigBuilder batchSize(String value) {
		config.put(ProducerConfig.BATCH_SIZE_CONFIG, value);
		return this;
	}

	public PulsarConfigBuilder blockOnBufferFull(String value) {
		config.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, value);
		return this;
	}

	public PulsarConfigBuilder bufferMemory(String value) {
		config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, value);
		//config.put(ConsumerConfig.TOTAL_BUFFER_MEMORY_CONFIG, value);
		return this;
	}

	public PulsarConfigBuilder clientId(String value) {
		config.put(ProducerConfig.CLIENT_ID_CONFIG, value);
		config.put(ConsumerConfig.CLIENT_ID_CONFIG, value);
		return this;
	}

	public PulsarConfigBuilder compressionType(String value) {
		config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, value);
		return this;
	}

	public PulsarConfigBuilder lingerMilli(String value) {
		config.put(ProducerConfig.LINGER_MS_CONFIG, value);
		return this;
	}

	public PulsarConfigBuilder maxInflightRequests(String value) {
		config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, value);
		return this;
	}

	public PulsarConfigBuilder maxRequestSize(String value) {
		config.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, value);
		return this;
	}

	public PulsarConfigBuilder metaDataFetchTimeout(String value) {
		config.put(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG, value);
		return this;
	}

	public PulsarConfigBuilder meteaDataMaxAge(String value) {
		config.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, value);
		return this;
	}

	public PulsarConfigBuilder metricReporterClasses(String value) {
		config.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, value);
		return this;
	}

	public PulsarConfigBuilder recieveBuffer(String value) {
		config.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, value);
		return this;
	}

	public PulsarConfigBuilder reconnectBackoffMilli(String value) {
		config.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, value);
		return this;
	}

	public PulsarConfigBuilder retrieCount(String value) {
		config.put(ProducerConfig.RETRIES_CONFIG, value);
		return this;
	}

	public PulsarConfigBuilder retryBackoffMillis(String value) {
		config.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, value);
		return this;
	}

	public PulsarConfigBuilder sendBuffer(String value) {
		config.put(ProducerConfig.SEND_BUFFER_CONFIG, value);
		return this;
	}

	public PulsarConfigBuilder timout(String value) {
		config.put(ProducerConfig.TIMEOUT_CONFIG, value);
		return this;
	}

	public PulsarConfigBuilder autoCommitInterval(String value) {
		config.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, value);
		return this;
	}

	public PulsarConfigBuilder autoOffsetReset(String value) {
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, value);
		return this;
	}

	public PulsarConfigBuilder bootstrapServer(String value) {
		config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, value);
		return this;
	}

	public PulsarConfigBuilder enableAuutoCommit(String value) {
		config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, value);
		return this;
	}

//	public PulsarConfigBuilder fetchBuffer(String value) {
//		config.put(ConsumerConfig.FETCH_BUFFER_CONFIG, value);
//		return this;
//	}

	public PulsarConfigBuilder fetchMaxWait(String value) {
		config.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, value);
		return this;
	}

	public PulsarConfigBuilder fetchMinBytes(String value) {
		config.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, value);
		return this;
	}

	public PulsarConfigBuilder groupId(String value) {
		config.put(ConsumerConfig.GROUP_ID_CONFIG, value);
		return this;
	}

//	public PulsarConfigBuilder heartbeatFrequency(String value) {
//		config.put(ConsumerConfig.HEARTBEAT_FREQUENCY, value);
//		return this;
//	}
//
//	public PulsarConfigBuilder metadataFetchTimeout(String value) {
//		config.put(ConsumerConfig.METADATA_FETCH_TIMEOUT_CONFIG, value);
//		return this;
//	}

	public PulsarConfigBuilder metricReporterClass(String value) {
		config.put(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, value);
		return this;
	}

	public PulsarConfigBuilder metricNumSamples(String value) {
		config.put(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG, value);
		return this;
	}

	public PulsarConfigBuilder metricSampleWindo(String value) {
		config.put(ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, value);
		return this;
	}

	public PulsarConfigBuilder reconnectBackoffMillis(String value) {
		config.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, value);
		return this;
	}
*/
//	public PulsarConfigBuilder sessionTimeout(String value) {
//		config.put(ConsumerConfig.SESSION_TIMEOUT_MS, value);
//		return this;
//	}
//
//	public PulsarConfigBuilder socketReceiveBuffer(String value) {
//		config.put(ConsumerConfig.SOCKET_RECEIVE_BUFFER_CONFIG, value);
//		return this;
//	}
//	
//	public PulsarConfigBuilder partitionAssignmentStrategy(String value) {
//		config.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY,value);
//		return this;
//	}
}
