package com.echostreams.pulsar.jms.client;

import com.echostreams.pulsar.jms.auth.AthenzAuthParams;
import com.echostreams.pulsar.jms.auth.TLSAuthParams;
import com.echostreams.pulsar.jms.config.PulsarConfigBuilder;
import com.echostreams.pulsar.jms.utils.ObjectSerializer;
import com.echostreams.pulsar.jms.utils.PulsarJMSException;
import com.echostreams.pulsar.jms.utils.StringDeserializer;
import com.echostreams.pulsar.jms.utils.StringSerializer;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.AuthenticationAthenz;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.jms.*;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PulsarConnectionFactory implements ConnectionFactory, QueueConnectionFactory, TopicConnectionFactory, Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarConnectionFactory.class);

    private static final String DEFAULT_AUTO_COMMIT_INTERVAL = "10000";
    private static final String DEFAULT_ASSIGNMENT_STRATEGY = "range";
    private static final String DEFAULT_AUTO_COMMIT = "false";
    private static final String DEFAULT_TIMOUT = "1000";
    private static String DEFAULT_BROKER = "pulsar://192.168.43.88:6650";
    private static String DEFAULT_VALUE_SERIALIZER = ObjectSerializer.class.getName();
    private static String DEFAULT_KEY_SERIALIZER = StringSerializer.class.getName();
    private static String DEFAULT_KEY_DESERIALIZER = StringDeserializer.class.getName();
    private PulsarConfigBuilder builder = new PulsarConfigBuilder();

    public PulsarConnectionFactory() {
    }

    public PulsarConnectionFactory(String brokerUrl) {
        this.DEFAULT_BROKER = brokerUrl;
    }

    /**
     * @return the builder
     */
    public PulsarConfigBuilder getBuilder() {
        return builder;
    }

    private Properties config;

    @PostConstruct
    public void initializeConfig() {
        /*builder.broker(DEFAULT_BROKER).valueSerializerClass(DEFAULT_VALUE_SERIALIZER)
                .keySerializerClass(DEFAULT_KEY_SERIALIZER).enableAuutoCommit(DEFAULT_AUTO_COMMIT)
                .autoCommitInterval(DEFAULT_AUTO_COMMIT_INTERVAL).keyDeserializerClass(DEFAULT_KEY_DESERIALIZER);
*/
    }

    public void setGroupId(String value) {
        // builder.groupId(value);
    }

    /*
     * (non-Javadoc)
     *
     * @see javax.jms.ConnectionFactory#createConnection()
     */
    @Override
    /*public Connection createConnection() throws JMSException {
        config = builder.build();
		return new PulsarConnection(config);
	}*/
    public Connection createConnection() throws JMSException {
        return createConnection(null, null);
    }

    /*
     * (non-Javadoc)
     *
     * @see javax.jms.ConnectionFactory#createConnection(java.lang.String,
     * java.lang.String)
     */
    @Override
    public Connection createConnection(String userName, String password)
            throws JMSException {
        return createPulsarConnection(userName, password);
    }

    /*
     * (non-Javadoc)
     *
     * @see javax.jms.ConnectionFactory#createContext()
     */
    @Override
    public JMSContext createContext() {
        return createContext(null, null, JMSContext.AUTO_ACKNOWLEDGE);
    }

    /*
     * (non-Javadoc)
     *
     * @see javax.jms.ConnectionFactory#createContext(java.lang.String,
     * java.lang.String)
     */
    @Override
    public JMSContext createContext(String userName, String password) {
        return createContext(userName, password, JMSContext.AUTO_ACKNOWLEDGE);
    }

    /*
     * (non-Javadoc)
     *
     * @see javax.jms.ConnectionFactory#createContext(java.lang.String,
     * java.lang.String, int)
     */
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

    /*
     * (non-Javadoc)
     *
     * @see javax.jms.ConnectionFactory#createContext(int)
     */
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
        PulsarClient client = null;
        try {
            client = PulsarClient.builder().serviceUrl(DEFAULT_BROKER).build();
        } catch (PulsarClientException e) {
            LOGGER.error("Could not create the connection :", e);
        }
        return new PulsarConnection(client);
    }

    private JMSContext createPulsarConnection(String userName, String password, int sessionMode) throws JMSException {
        PulsarClient client = null;
        try {
            client = PulsarClient.builder().serviceUrl(DEFAULT_BROKER).build();
        } catch (PulsarClientException e) {
            LOGGER.error("Could not create the connection :", e);
        }
        return new PulsarJMSContext(client);
    }

    public static PulsarClient tlsAuthentication(TLSAuthParams tlsAuthParams) {

        Map<String, String> authParams = new HashMap<>();
        authParams.put("tlsCertFile", tlsAuthParams.getTlsCertFile());
        authParams.put("tlsKeyFile", tlsAuthParams.getTlsKeyFile());

        Authentication tlsAuth;
        PulsarClient client = null;
        try {
            tlsAuth = AuthenticationFactory
                    .create(AuthenticationTls.class.getName(), authParams);

            client = PulsarClient.builder()
                    .serviceUrl(tlsAuthParams.getServiceUrl())
                    .enableTls(true)
                    .tlsTrustCertsFilePath(tlsAuthParams.getTlsTrustCertsFilePath())
                    .authentication(tlsAuth)
                    .build();

        } catch (PulsarClientException e) {
            new PulsarJMSException("TLS Authentication Error", e.getMessage());
        }

        return client;
    }

    public static PulsarClient athenzAuthentication(AthenzAuthParams athenzAuthParams) {
        Map<String, String> authParams = new HashMap<>();
        authParams.put("tenantDomain", athenzAuthParams.getTenantDomain()); // Tenant domain name
        authParams.put("tenantService", athenzAuthParams.getTenantService()); // Tenant service name
        authParams.put("providerDomain", athenzAuthParams.getProviderDomain()); // Provider domain name
        authParams.put("privateKey", athenzAuthParams.getPrivateKey()); // Tenant private key path
        authParams.put("keyId", athenzAuthParams.getKeyId()); // Key id for the tenant private key (optional, default: "0")

        Authentication athenzAuth;
        PulsarClient client = null;
        try {
            athenzAuth = AuthenticationFactory
                    .create(AuthenticationAthenz.class.getName(), authParams);

            client = PulsarClient.builder()
                    .serviceUrl(athenzAuthParams.getServiceUrl())
                    .enableTls(true)
                    .tlsTrustCertsFilePath(athenzAuthParams.getTlsTrustCertsFilePath())
                    .authentication(athenzAuth)
                    .build();
        } catch (PulsarClientException e) {
            new PulsarJMSException("Athenz Authentication Error", e.getMessage());
        }
        return client;
    }

}
