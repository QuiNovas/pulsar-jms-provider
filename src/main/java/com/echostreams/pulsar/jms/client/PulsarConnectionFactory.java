package com.echostreams.pulsar.jms.client;

import com.echostreams.pulsar.jms.config.PulsarConfig;
import com.echostreams.pulsar.jms.config.PulsarConstants;
import com.echostreams.pulsar.jms.exceptions.PulsarJMSException;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.auth.AuthenticationAthenz;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class PulsarConnectionFactory implements ConnectionFactory, QueueConnectionFactory, TopicConnectionFactory, Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarConnectionFactory.class);
    private static final long serialVersionUID = 5725538819716172914L;

    public PulsarConnectionFactory() {
    }

    public PulsarConnectionFactory(String brokerUrl) {
        PulsarConfig.SERVICE_URL = brokerUrl;
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
        return new PulsarConnection(prepareClientWithoutAuth());
    }

    private JMSContext createPulsarConnection(String userName, String password, int sessionMode) throws JMSException {
        if (PulsarConstants.TLS.equals(PulsarConfig.ENABLE_AUTH)) {
            return new PulsarJMSContext(tlsAuthentication());
        }
        if (PulsarConstants.ATHENZ.equals(PulsarConfig.ENABLE_AUTH)) {
            return new PulsarJMSContext(athenzAuthentication());
        }
        return new PulsarJMSContext(prepareClientWithoutAuth());
    }

    public PulsarClient tlsAuthentication() {
        Map<String, String> authParams = new HashMap<>();
        authParams.put("tlsCertFile", PulsarConfig.TLS_CERT_FILE);
        authParams.put("tlsKeyFile", PulsarConfig.TLS_KEY_FILE);

        Authentication tlsAuth;
        PulsarClient client = null;
        try {
            tlsAuth = AuthenticationFactory
                    .create(AuthenticationTls.class.getName(), authParams);
            client = prepareClient(tlsAuth);
        } catch (PulsarClientException e) {
            new PulsarJMSException("TLS Authentication Error", e.getMessage());
        }

        return client;
    }

    public PulsarClient athenzAuthentication() {
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
            client = prepareClient(athenzAuth);
        } catch (PulsarClientException e) {
            new PulsarJMSException("Athenz Authentication Error", e.getMessage());
        }
        return client;
    }

    private PulsarClient prepareClientWithoutAuth() {
        PulsarClient client = null;
        try {
            if (PulsarConfig.clientConfig == null) {
                return PulsarClient.builder().serviceUrl(PulsarConfig.SERVICE_URL).build();
            }
            client = PulsarConfig.clientConfig.build();

        } catch (PulsarClientException e) {
            LOGGER.error("Could not create the connection :", e);
        }
        return client;
    }

    private PulsarClient prepareClient(Authentication tlsOrAthenzAuth) throws PulsarClientException {
        if (PulsarConfig.clientConfig == null) {
            return PulsarClient.builder()
                    .serviceUrl(PulsarConfig.SERVICE_URL)
                    .tlsTrustCertsFilePath(PulsarConfig.TLS_TRUST_CERTS_FILEPATH)
                    .authentication(tlsOrAthenzAuth)
                    .build();
        }
        ClientBuilder clientBuilder = PulsarConfig.clientConfig;
        return clientBuilder.tlsTrustCertsFilePath(PulsarConfig.TLS_TRUST_CERTS_FILEPATH)
                .authentication(tlsOrAthenzAuth)
                .build();
    }

}
