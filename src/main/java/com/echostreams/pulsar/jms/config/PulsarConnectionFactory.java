package com.echostreams.pulsar.jms.config;

import com.echostreams.pulsar.jms.common.AbstractConnectionFactory;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.JMSException;
import java.net.URI;
import java.net.URISyntaxException;

public class PulsarConnectionFactory extends AbstractConnectionFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarConnectionFactory.class);

    protected String brokerURL;
    protected String userName;
    protected String password;

    public PulsarConnectionFactory() {
        this(PulsarConfiguration.getBrokerUrl());
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

    private static URI createURI(String brokerURL) {
        try {
            return new URI(brokerURL);
        } catch (URISyntaxException var2) {
            throw (IllegalArgumentException) (new IllegalArgumentException("Invalid broker URI: " + brokerURL)).initCause(var2);
        }
    }

}
