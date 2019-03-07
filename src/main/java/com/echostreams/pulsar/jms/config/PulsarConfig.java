package com.echostreams.pulsar.jms.config;

import com.echostreams.pulsar.jms.exceptions.PulsarJMSException;

import java.io.InputStream;
import java.util.Properties;

public class PulsarConfig {

    public static PulsarConfig pulsarConfig;
    public Properties prop;

    // PulsarClient Connection
    public static String SERVICE_URL = "pulsar://localhost:8080"; // or http://localhost:6650

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

    public static synchronized void initializeConfig(String configFilePath) {
        if (pulsarConfig == null) {
            pulsarConfig = new PulsarConfig(configFilePath);
        }

    }

    public PulsarConfig(String configFilePath) {
        prop = new Properties();
        InputStream inputStream;

        try {
            inputStream = PulsarConfig.class.getResourceAsStream(configFilePath);
            if (inputStream != null) {
                prop.load(inputStream);
                readPropertyValue(prop);
            }
        } catch (java.io.IOException e) {
            new PulsarJMSException("Exception during reading property file:", e.getMessage());
        }
    }

    private void readPropertyValue(Properties properties) {
        this.SERVICE_URL = properties.getProperty("pulsar.serviceUrl");
        this.PULSAR_TOPIC = properties.getProperty("pulsar.topic");
        this.TLS_CERT_FILE = properties.getProperty("tls.certFile");
        this.TLS_KEY_FILE = properties.getProperty("tls.KeyFile");
        this.TLS_TRUST_CERTS_FILEPATH = properties.getProperty("tls.TrustCertsFilePath");
        this.ATHENZ_TENANT_DOMAIN = properties.getProperty("athenz.tenantDomain");
        this.ATHENZ_TENANT_SERVICE = properties.getProperty("athenz.tenantService");
        this.ATHENZ_PROVIDER_DOMAIN = properties.getProperty("athenz.providerDomain");
        this.ATHENZ_PRIVATE_KEY = properties.getProperty("athenz.privateKey");
        this.ATHENZ_KEY_ID = properties.getProperty("athenz.keyId", "0");
    }

}
