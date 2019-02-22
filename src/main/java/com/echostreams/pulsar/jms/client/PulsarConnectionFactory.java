package com.echostreams.pulsar.jms.client;

import com.echostreams.pulsar.jms.ObjectSerializer;
import com.echostreams.pulsar.jms.PulsarConfigBuilder;
import com.echostreams.pulsar.jms.auth.AthenzAuthParams;
import com.echostreams.pulsar.jms.auth.TLSAuthParams;
import com.echostreams.pulsar.jms.utils.PulsarJMSException;
import com.echostreams.pulsar.jms.utils.StringDeserializer;
import com.echostreams.pulsar.jms.utils.StringSerializer;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.AuthenticationAthenz;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;

import javax.annotation.PostConstruct;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PulsarConnectionFactory implements ConnectionFactory {
    private static final String DEFAULT_AUTO_COMMIT_INTERVAL = "10000";
    private static final String DEFAULT_ASSIGNMENT_STRATEGY = "range";
    private static final String DEFAULT_AUTO_COMMIT = "false";
    private static final String DEFAULT_TIMOUT = "1000";
    private static String DEFAULT_BROKER = "localhost:9092";
    private static String DEFAULT_VALUE_SERIALIZER = ObjectSerializer.class.getName();
    private static String DEFAULT_KEY_SERIALIZER = StringSerializer.class.getName();
    private static String DEFAULT_KEY_DESERIALIZER = StringDeserializer.class.getName();
    private PulsarConfigBuilder builder = new PulsarConfigBuilder();

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
        PulsarClient client = null;
        try {
            client = PulsarClient.builder()
                    .serviceUrl("http://localhost:8080")
                    .build();
        } catch (PulsarClientException e) {
            //LOGGER.error("Could not create the connection :", e);
        }
        return new PulsarConnection(client);
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
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     *
     * @see javax.jms.ConnectionFactory#createContext()
     */
    @Override
    public JMSContext createContext() {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     *
     * @see javax.jms.ConnectionFactory#createContext(java.lang.String,
     * java.lang.String)
     */
    @Override
    public JMSContext createContext(String userName, String password) {
        // TODO Auto-generated method stub
        return null;
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
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     *
     * @see javax.jms.ConnectionFactory#createContext(int)
     */
    @Override
    public JMSContext createContext(int sessionMode) {
        // TODO Auto-generated method stub
        return null;
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
