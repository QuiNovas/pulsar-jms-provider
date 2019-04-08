package com.echostreams.pulsar.jms.config;

import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.ConsumerBuilderImpl;
import org.apache.pulsar.client.impl.ProducerBuilderImpl;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class PulsarConfig {

    public static PulsarConfig pulsarConfig;
    public Properties prop;
    public static ClientBuilder clientConfig = null;
    public static ProducerBuilderImpl producerConfig = null;
    public static ConsumerBuilderImpl consumerConfig = null;

    /*
     * PulsarClient Config
     */
    // PulsarClient Connection
    public static String ENABLE_AUTH = ""; // empty-no auth, TLS-tls auth, ATHENZ-athenz auth
    public static String SERVICE_URL;
    public static int OPERATION_TIMEOUT_MS = 30000;
    public static long STATS_INTERVAL_SECONDS = 60;
    public static int NUM_IO_THREADS = 1;
    public static int NUM_LISTENER_THREADS = 1;
    public static int CONNECTIONS_PER_BROKER = 1;
    public static boolean USE_TCP_NODELAY = true;
    public static boolean TLS_ALLOW_INSECURE_CONNECTION = false;
    public static boolean TLS_HOSTNAME_VERIFICATION_ENABLE = false;
    public static int CONCURRENT_LOOKUP_REQUEST = 5000;
    public static int MAX_LOOKUP_REQUEST = 50000;
    public static int MAX_NUMBER_OF_REJECTED_REQUEST_PER_CONNECTION = 50;
    public static int KEEP_ALIVE_INTERVAL_SECONDS = 50;

    //pulsar topic
    public static String PULSAR_TOPIC = "persistent://tenant/app1/topic-1"; //persistent://tenant/namespace/topic

    // TLS authentication config
    public static String TLS_CERT_FILE = "/path/to/client-cert.pem";
    public static String TLS_KEY_FILE = "/path/to/client-key.pem";
    public static String TLS_TRUST_CERTS_FILEPATH = "/path/to/client-cert.pem";

    //Athenz authentication config
    public static String ATHENZ_TENANT_DOMAIN = "DEMO";
    public static String ATHENZ_TENANT_SERVICE = "DEMO_APP";
    public static String ATHENZ_PROVIDER_DOMAIN = "PULSAR";
    public static String ATHENZ_PRIVATE_KEY = "file:///path/to/private.pem";
    public static String ATHENZ_KEY_ID = "V1";    // optional, default is 0

    //Token authentication config
    public static String TOKEN_AUTH_PARAMS;

    public static synchronized void initializeConfig(String configFilePath) throws IOException {
        if (pulsarConfig == null) {
            pulsarConfig = new PulsarConfig(configFilePath);
        }

    }

    public PulsarConfig(String configFilePath) throws IOException {
        prop = new Properties();
        InputStream inputStream;

        inputStream = PulsarConfig.class.getResourceAsStream(configFilePath);
        if (inputStream != null) {
            prop.load(inputStream);
            readPropertyValue(prop);
        }
    }

    private void readPropertyValue(Properties properties) {
        ENABLE_AUTH = properties.getProperty("pulsar.enabledAuth");
        SERVICE_URL = properties.getProperty("pulsar.serviceUrl");
        OPERATION_TIMEOUT_MS = Integer.parseInt(properties.getProperty("pulsar.operationTimeoutMs"));
        STATS_INTERVAL_SECONDS = Long.parseLong(properties.getProperty("pulsar.statsIntervalSeconds"));
        NUM_IO_THREADS = Integer.parseInt(properties.getProperty("pulsar.numIoThreads"));
        NUM_LISTENER_THREADS = Integer.parseInt(properties.getProperty("pulsar.numListenerThreads"));
        CONNECTIONS_PER_BROKER = Integer.parseInt(properties.getProperty("pulsar.connectionsPerBroker"));
        USE_TCP_NODELAY = Boolean.parseBoolean(properties.getProperty("pulsar.useTcpNoDelay"));
        TLS_ALLOW_INSECURE_CONNECTION = Boolean.parseBoolean(properties.getProperty("pulsar.tlsAllowInsecureConnection"));
        TLS_HOSTNAME_VERIFICATION_ENABLE = Boolean.parseBoolean(properties.getProperty("pulsar.tlsHostnameVerificationEnable"));
        CONCURRENT_LOOKUP_REQUEST = Integer.parseInt(properties.getProperty("pulsar.concurrentLookupRequest"));
        MAX_LOOKUP_REQUEST = Integer.parseInt(properties.getProperty("pulsar.maxLookupRequest"));
        MAX_NUMBER_OF_REJECTED_REQUEST_PER_CONNECTION = Integer.parseInt(properties.getProperty("pulsar.maxNumberOfRejectedRequestPerConnection"));
        KEEP_ALIVE_INTERVAL_SECONDS = Integer.parseInt(properties.getProperty("pulsar.keepAliveIntervalSeconds"));
        PULSAR_TOPIC = properties.getProperty("pulsar.topic");
        TLS_CERT_FILE = properties.getProperty("pulsar.tlsCertFile");
        TLS_KEY_FILE = properties.getProperty("pulsar.tlsKeyFile");
        TLS_TRUST_CERTS_FILEPATH = properties.getProperty("pulsar.tlsTrustCertsFilePath");
        ATHENZ_TENANT_DOMAIN = properties.getProperty("pulsar.athenz.tenantDomain");
        ATHENZ_TENANT_SERVICE = properties.getProperty("pulsar.athenz.tenantService");
        ATHENZ_PROVIDER_DOMAIN = properties.getProperty("pulsar.athenz.providerDomain");
        ATHENZ_PRIVATE_KEY = properties.getProperty("pulsar.athenz.privateKey");
        ATHENZ_KEY_ID = properties.getProperty("pulsar.athenz.keyId", "0");
        TOKEN_AUTH_PARAMS = properties.getProperty("pulsar.authParams");
    }

    public void changeDefaultClientConfigFromConfigFile() {
        clientConfig = PulsarClient.builder();
        clientConfig.serviceUrl(SERVICE_URL);
        clientConfig.operationTimeout(OPERATION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        clientConfig.statsInterval(STATS_INTERVAL_SECONDS, TimeUnit.SECONDS);
        clientConfig.ioThreads(NUM_IO_THREADS);
        clientConfig.listenerThreads(NUM_LISTENER_THREADS);
        clientConfig.connectionsPerBroker(CONNECTIONS_PER_BROKER);
        clientConfig.enableTcpNoDelay(USE_TCP_NODELAY);
        clientConfig.allowTlsInsecureConnection(TLS_ALLOW_INSECURE_CONNECTION);
        clientConfig.enableTlsHostnameVerification(TLS_HOSTNAME_VERIFICATION_ENABLE);
        clientConfig.maxConcurrentLookupRequests(CONCURRENT_LOOKUP_REQUEST);
        clientConfig.maxNumberOfRejectedRequestPerConnection(MAX_NUMBER_OF_REJECTED_REQUEST_PER_CONNECTION);
        clientConfig.keepAliveInterval(KEEP_ALIVE_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    public static void changeDefaultClientConfig(ClientBuilder clientBuilder) {
        clientConfig = PulsarClient.builder();
        clientConfig = clientBuilder;
    }

    public static void changeDefaultProducerConfig(ProducerBuilderImpl producerBuilderImpl) {
        producerConfig = producerBuilderImpl;
    }

    public static void changeDefaultConsumerConfig(ConsumerBuilderImpl consumerBuilder) {
        consumerConfig = consumerBuilder;
    }

}
