package com.echostreams.pulsar.jms.utils;

import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtils {

    public static PropertiesUtils propertiesUtils;
    public Properties prop;

    static {
        if (propertiesUtils == null) {
            propertiesUtils = new PropertiesUtils();
        }

    }

    private PropertiesUtils() {
        prop = new Properties();
        InputStream inputStream;

        try {
            inputStream = PropertiesUtils.class.getResourceAsStream("/application.properties");
            if (inputStream != null) {
                prop.load(inputStream);
            }
        } catch (java.io.IOException e) {
            new PulsarJMSException("Exception during reading property file:", e.getMessage());
        }

    }


    public String getServiceUrl() {
        return prop.getProperty("pulsar.serviceUrl");
    }

    public String getTopicName() {
        return prop.getProperty("pulsar.topic");
    }

    public String getTLSCertFile() {
        return prop.getProperty("tls.certFile");
    }

    public String getTLSKeyFile() {
        return prop.getProperty("tls.KeyFile");
    }

    public String getTLSTrustCertsFilePath() {
        return prop.getProperty("tls.TrustCertsFilePath");
    }

    public String getAthenzTenantDomain() {
        return prop.getProperty("athenz.tenantDomain");
    }

    public String getAthenzTenantService() {
        return prop.getProperty("athenz.tenantService");
    }

    public String getAthenzProviderDomain() {
        return prop.getProperty("athenz.providerDomain");
    }

    public String getAthenzPrivateKey() {
        return prop.getProperty("athenz.privateKey");
    }

    public String getAthenzKeyId() {
        return prop.getProperty("athenz.keyId");
    }

}
