package com.echostreams.pulsar.jms.config;

import com.echostreams.pulsar.jms.common.AbstractConnection;

import javax.jms.*;

public class PulsarConnection extends AbstractConnection {

    public PulsarConnection(String clientID) {
        super(clientID);
    }

    @Override
    public void deleteTemporaryQueue(String queueName) throws JMSException {

    }

    @Override
    public void deleteTemporaryTopic(String topicName) throws JMSException {

    }

    @Override
    public Session createSession(boolean b, int i) throws JMSException {
        return null;
    }

    @Override
    public ConnectionMetaData getMetaData() throws JMSException {
        return null;
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Destination destination, String s, ServerSessionPool serverSessionPool, int i) throws JMSException {
        return null;
    }

    @Override
    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String s, String s1, ServerSessionPool serverSessionPool, int i) throws JMSException {
        return null;
    }
}
