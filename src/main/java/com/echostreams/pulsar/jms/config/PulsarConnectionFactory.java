package com.echostreams.pulsar.jms.config;

import com.echostreams.pulsar.jms.auth.AthenzAuthParams;
import com.echostreams.pulsar.jms.auth.TLSAuthParams;
import com.echostreams.pulsar.jms.common.AbstractConnectionFactory;
import com.echostreams.pulsar.jms.utils.PulsarJMSException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.AuthenticationAthenz;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.JMSException;
import java.util.HashMap;
import java.util.Map;

public class PulsarConnectionFactory extends AbstractConnectionFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarConnectionFactory.class);

    protected String brokerURL;
    protected String userName;
    protected String password;

    public PulsarConnectionFactory() {
        this(PulsarConfiguration.WEB_SERVICE_URL);
    }

    public PulsarConnectionFactory(String brokerURL) {

        this.brokerURL = brokerURL;
    }


    public Connection createConnection() throws JMSException {
        return this.createPulsarConnection();
    }

    public Connection createConnection(String userName, String password) throws JMSException {
        return this.createPulsarConnection(userName, password);
    }

    protected PulsarConnection createPulsarConnection() throws JMSException {
        return this.createPulsarConnection(this.userName, this.password);
    }

    protected PulsarConnection createPulsarConnection(String userName, String password) throws JMSException {

        PulsarConnection connection = null;
        PulsarClient client = null;

        client = this.createPulsarClient();
        return null;
    }

    private PulsarClient createPulsarClient() {
        try {
            return PulsarClient.builder()
                    .serviceUrl(this.brokerURL)
                    .build();
        } catch (PulsarClientException e) {
            LOGGER.error("Could not create the connection :", e);
        }
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
