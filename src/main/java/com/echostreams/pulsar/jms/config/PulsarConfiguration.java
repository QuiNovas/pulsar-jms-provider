package com.echostreams.pulsar.jms.config;

public interface PulsarConfiguration {

    // Network defaults
    public static final String WEB_SERVICE_URL = "http://localhost:8080";

    // Transport protocol version
    public static final int TRANSPORT_PROTOCOL_VERSION = 9;

    // JNDI related constants
    //public static final String JNDI_CONTEXT_FACTORY = FFMQInitialContextFactory.class.getName();
    public static final String JNDI_CONNECTION_FACTORY_NAME = "factory/ConnectionFactory";
    public static final String JNDI_QUEUE_CONNECTION_FACTORY_NAME = "factory/QueueConnectionFactory";
    public static final String JNDI_TOPIC_CONNECTION_FACTORY_NAME = "factory/TopicConnectionFactory";
    public static final String JNDI_ENV_CLIENT_ID = "ffmq.naming.clientID";

    // Max name size
    public static final int MAX_QUEUE_NAME_SIZE = 128;
    public static final int MAX_TOPIC_NAME_SIZE = 196;

    // Administration queues
    public static final String ADM_REQUEST_QUEUE = "_PULSAR_ADM_REQUEST";
    public static final String ADM_REPLY_QUEUE   = "_PULSAR_ADM_REPLY";
}
