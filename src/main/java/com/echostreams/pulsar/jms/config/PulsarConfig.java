package com.echostreams.pulsar.jms.config;

public interface PulsarConfig {
    // Network defaults
    public static final String SERVICE_URL = "http://localhost:8080"; // or pulsar://localhost:6650

    //pulsar topic
    public static final String PULSAR_TOPIC = "persistent://tenant/app1/topic-1"; //persistent://tenant/namespace/topic

    // TLS authentication config
    public static final String TLS_CERT_FILE = "/path/to/client-cert.pem";
    public static final String TLS_KEY_FILE = "/path/to/client-key.pem";
    public static final String TLS_TRUST_CERTS_FILEPATH = "/path/to/client-cert.pem";
    public static final String TLS_CERTFILE = "/path/to/client-cert.pem";

    //Athenz authentication config
    public static final String ATHENZ_TENANT_DOMAIN = "DEMO";
    public static final String ATHENZ_TENANT_SERVICE = "DEMO_APP";
    public static final String ATHENZ_PROVIDER_DOMAIN = "PULSAR";
    public static final String ATHENZ_PRIVATE_KEY = "file:///path/to/private.pem";
    public static final String ATHENZ_KEY_ID = "V1";    // optional, default is 0

    // Transport protocol version
    public static final int TRANSPORT_PROTOCOL_VERSION = 9;

    // JNDI related constants
    //public static final String JNDI_CONTEXT_FACTORY = PulsarJMSInitialContextFactory.class.getName();
    public static final String JNDI_CONNECTION_FACTORY_NAME = "pulsar/ConnectionFactory";
    public static final String JNDI_QUEUE_CONNECTION_FACTORY_NAME = "pulsar/QueueConnectionFactory";
    public static final String JNDI_TOPIC_CONNECTION_FACTORY_NAME = "pulsar/TopicConnectionFactory";
    public static final String JNDI_ENV_CLIENT_ID = "pulsar.naming.clientID";

    // Max name size
    public static final int MAX_QUEUE_NAME_SIZE = 128;
    public static final int MAX_TOPIC_NAME_SIZE = 196;

    // Administration queues
    public static final String ADM_REQUEST_QUEUE = "_PULSAR_ADM_REQUEST";
    public static final String ADM_REPLY_QUEUE = "_PULSAR_ADM_REPLY";

    public static final String QUEUE_PREFIX = "queue/";
    public static final String TOPIC_PREFIX = "topic/";
    public static final String SERVER = "server";

    // Message Type
    public static final String TEXT_MESSAGE = "TextMessage";
    public static final String BYTES_MESSAGE = "BytesMessage";
    public static final String MAP_MESSAGE = "MapMessage";
    public static final String OBJECT_MESSAGE = "ObjectMessage";
    public static final String STREAM_MESSAGE = "StreamMessage";
    public static final String BLOB_MESSAGE = "BlobMessage";

}
