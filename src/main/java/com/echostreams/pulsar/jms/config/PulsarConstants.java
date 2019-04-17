package com.echostreams.pulsar.jms.config;

import com.echostreams.pulsar.jms.jndi.PulsarInitialContextFactory;

public class PulsarConstants {

    //PULSAR AUTH
    public static final String TLS = "TLS";
    public static final String ATHENZ = "ATHENZ";
    public static final String TOKEN = "TOKEN";

    // JNDI related constants
    public static final String JNDI_CONTEXT_FACTORY = PulsarInitialContextFactory.class.getName();
    public static final String JNDI_ENV_CLIENT_ID = "pulsar.naming.clientID";

    // Message Type
    public static final String TEXT_MESSAGE = "TextMessage";
    public static final String BYTES_MESSAGE = "BytesMessage";
    public static final String MAP_MESSAGE = "MapMessage";
    public static final String OBJECT_MESSAGE = "ObjectMessage";
    public static final String STREAM_MESSAGE = "StreamMessage";
    public static final String BLOB_MESSAGE = "BlobMessage";
}
