package com.echostreams.pulsar.jms.common;

import com.echostreams.pulsar.jms.config.PulsarConnection;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;

public abstract class AbstractConnectionFactory implements ConnectionFactory {

    @Override
    public Connection createConnection() throws JMSException {
        return new PulsarConnection();
    }

    @Override
    public Connection createConnection(String userName, String password) throws JMSException {
        return new PulsarConnection();
    }
}
