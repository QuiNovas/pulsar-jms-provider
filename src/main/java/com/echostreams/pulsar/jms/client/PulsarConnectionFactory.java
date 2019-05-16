package com.echostreams.pulsar.jms.client;

import com.echostreams.pulsar.jms.config.PulsarConfig;
import com.echostreams.pulsar.jms.config.PulsarConstants;
import com.echostreams.pulsar.jms.exceptions.PulsarJMSException;
import com.echostreams.pulsar.jms.jndi.JNDIStorable;
import com.echostreams.pulsar.jms.utils.PropertyUtils;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.auth.AuthenticationAthenz;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class PulsarConnectionFactory extends JNDIStorable implements ConnectionFactory, QueueConnectionFactory, TopicConnectionFactory, Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarConnectionFactory.class);
    private static final long serialVersionUID = 5725538819716172914L;

    public static String DEFAULT_BROKER_URL = "pulsar://localhost:6500";
    public static String DEFAULT_BROKER_PROP = "brokerUrl";

    private static String SERVICE_URL;

    public PulsarConnectionFactory() {
    }

    public PulsarConnectionFactory(String brokerUrl) {
        SERVICE_URL = brokerUrl;
    }

    public Connection createConnection() throws JMSException {
        return createConnection(null, null);
    }

    @Override
    public Connection createConnection(String userName, String password)
            throws JMSException {
        return createPulsarConnection(userName, password);
    }

    @Override
    public JMSContext createContext() {
        return createContext(null, null, JMSContext.AUTO_ACKNOWLEDGE);
    }

    @Override
    public JMSContext createContext(String userName, String password) {
        return createContext(userName, password, JMSContext.AUTO_ACKNOWLEDGE);
    }

    @Override
    public JMSContext createContext(String userName, String password,
                                    int sessionMode) {
        JMSContext jmsContext = null;
        try {
            jmsContext = createPulsarConnection(userName, password, sessionMode);
        } catch (JMSException e) {
            LOGGER.error("createContext error ", e);
        }
        return jmsContext;
    }

    @Override
    public JMSContext createContext(int sessionMode) {
        return createContext(null, null, sessionMode);
    }

    @Override
    public QueueConnection createQueueConnection() throws JMSException {
        return (QueueConnection) createConnection();
    }

    @Override
    public QueueConnection createQueueConnection(String userName, String password) throws JMSException {
        return (QueueConnection) createConnection(userName, password);
    }

    @Override
    public TopicConnection createTopicConnection() throws JMSException {
        return (TopicConnection) createConnection();
    }

    @Override
    public TopicConnection createTopicConnection(String userName, String password) throws JMSException {
        return (TopicConnection) createConnection(userName, password);
    }

    private Connection createPulsarConnection(String userName, String password) throws JMSException {
        if (PulsarConstants.TLS.equals(PulsarConfig.ENABLE_AUTH)) {
            return new PulsarConnection(tlsAuthentication());
        }
        if (PulsarConstants.ATHENZ.equals(PulsarConfig.ENABLE_AUTH)) {
            return new PulsarConnection(athenzAuthentication());
        }
        if (PulsarConstants.TOKEN.equals(PulsarConfig.ENABLE_AUTH)) {
            return new PulsarConnection(tokenBasedAuthentication());
        }

        return new PulsarConnection(prepareClientWithoutAuth());
    }

    private JMSContext createPulsarConnection(String userName, String password, int sessionMode) throws JMSException {
        if (PulsarConstants.TLS.equals(PulsarConfig.ENABLE_AUTH)) {
            return new PulsarJMSContext(tlsAuthentication(), sessionMode);
        }
        if (PulsarConstants.ATHENZ.equals(PulsarConfig.ENABLE_AUTH)) {
            return new PulsarJMSContext(athenzAuthentication(), sessionMode);
        }
        if (PulsarConstants.TOKEN.equals(PulsarConfig.ENABLE_AUTH)) {
            return new PulsarJMSContext(tokenBasedAuthentication(), sessionMode);
        }

        return new PulsarJMSContext(prepareClientWithoutAuth(), sessionMode);
    }

    private PulsarClient tlsAuthentication() {
        LOGGER.info("Try Connecting to PulsarClient with TLS Auth");
        Map<String, String> authParams = new HashMap<>();
        authParams.put("tlsCertFile", PulsarConfig.TLS_CERT_FILE);
        authParams.put("tlsKeyFile", PulsarConfig.TLS_KEY_FILE);

        Authentication tlsAuth;
        PulsarClient client = null;
        try {
            tlsAuth = AuthenticationFactory
                    .create(AuthenticationTls.class.getName(), authParams);
            client = prepareClientWithAuth(tlsAuth);
        } catch (PulsarClientException e) {
            new PulsarJMSException("TLS Authentication Error", e.getMessage());
        }

        return client;
    }

    private PulsarClient athenzAuthentication() {
        LOGGER.info("Try Connecting to PulsarClient with Athenz Auth");
        Map<String, String> authParams = new HashMap<>();
        authParams.put("tenantDomain", PulsarConfig.ATHENZ_TENANT_DOMAIN); // Tenant domain name
        authParams.put("tenantService", PulsarConfig.ATHENZ_TENANT_SERVICE); // Tenant service name
        authParams.put("providerDomain", PulsarConfig.ATHENZ_PROVIDER_DOMAIN); // Provider domain name
        authParams.put("privateKey", PulsarConfig.ATHENZ_PRIVATE_KEY); // Tenant private key path
        authParams.put("keyId", PulsarConfig.ATHENZ_KEY_ID); // Key id for the tenant private key (optional, default: "0")

        Authentication athenzAuth;
        PulsarClient client = null;
        try {
            athenzAuth = AuthenticationFactory
                    .create(AuthenticationAthenz.class.getName(), authParams);
            client = prepareClientWithAuth(athenzAuth);
        } catch (PulsarClientException e) {
            new PulsarJMSException("Athenz Authentication Error", e.getMessage());
        }
        return client;
    }

    private PulsarClient tokenBasedAuthentication() {
        LOGGER.info("Try Connecting to PulsarClient with Token Auth");

        ClientBuilder clientBuilder;
        PulsarClient client = null;
        try {
            if (PulsarConfig.clientConfig == null) {
                clientBuilder = PulsarClient.builder()
                        .serviceUrl(getUpdatedServiceUrl());
            } else {
                clientBuilder = PulsarConfig.clientConfig;
            }

            if (PulsarConfig.TOKEN_AUTH_PARAMS.isEmpty()) {
                throw new IllegalArgumentException("No token auth param found, check config! We must provide token value!");
            }

            // token value is directly passed
            if (PulsarConfig.TOKEN_AUTH_PARAMS.startsWith("token:")) {
                return clientBuilder
                        .authentication(
                                AuthenticationFactory.token(PulsarConfig.TOKEN_AUTH_PARAMS))
                        .build();
            }

            // token value is passed into file
            client = clientBuilder
                    .authentication(
                            AuthenticationFactory.token(() -> {
                                // Read token from custom source
                                return readTokenFromFile();
                            }))
                    .build();

        } catch (PulsarClientException e) {
            new PulsarJMSException("Token Authentication Error", e.getMessage());
        }
        return client;
    }

    private String readTokenFromFile() {
        try {
            if (new File(PulsarConfig.TOKEN_AUTH_PARAMS).isFile())
                return new String(Files.readAllBytes(Paths.get(PulsarConfig.TOKEN_AUTH_PARAMS)));
            else
                throw new FileNotFoundException(PulsarConfig.TOKEN_AUTH_PARAMS + " doesn't exist or not valid file");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private PulsarClient prepareClientWithoutAuth() {
        PulsarClient client = null;
        try {
            if (PulsarConfig.clientConfig == null) {
                return PulsarClient.builder().serviceUrl(getUpdatedServiceUrl()).build();
            }
            client = PulsarConfig.clientConfig.build();

        } catch (PulsarClientException e) {
            LOGGER.error("Could not create the connection :", e);
        }
        return client;
    }

    private PulsarClient prepareClientWithAuth(Authentication tlsOrAthenzAuth) throws PulsarClientException {
        if (PulsarConfig.clientConfig == null) {
            return PulsarClient.builder()
                    .serviceUrl(getUpdatedServiceUrl())
                    .tlsTrustCertsFilePath(PulsarConfig.TLS_TRUST_CERTS_FILEPATH)
                    .authentication(tlsOrAthenzAuth)
                    .build();
        }
        return PulsarConfig.clientConfig.tlsTrustCertsFilePath(PulsarConfig.TLS_TRUST_CERTS_FILEPATH)
                .authentication(tlsOrAthenzAuth)
                .build();
    }

    /*
     * Checking if already set by JNDI context, else take from config file else take default value
     */
    private String getUpdatedServiceUrl() {
        if (SERVICE_URL == null) {
            SERVICE_URL = (PulsarConfig.SERVICE_URL == null) ? DEFAULT_BROKER_URL : PulsarConfig.SERVICE_URL;
        }

        return SERVICE_URL;
    }

    /**
     * Set the properties that will represent the instance in JNDI
     *
     * @param props The properties to use when building the new isntance.
     * @return a new, unmodifiable, map containing any unused properties, or empty if none were.
     */
    @Override
    protected Map<String, String> buildFromProperties(Map<String, String> props) {
        String remoteURI = props.remove(DEFAULT_BROKER_PROP);
        if (remoteURI != null) {
            SERVICE_URL = remoteURI;
        }

        return PropertyUtils.setProperties(this, props);
    }

    /**
     * Initialize the instance from properties stored in JNDI
     *
     * @param props The properties to use when initializing the new instance.
     */
    @Override
    protected void populateProperties(Map<String, String> props) {
        try {
            Map<String, String> result = PropertyUtils.getProperties(this);
            props.putAll(result);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
