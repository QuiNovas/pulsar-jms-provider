package com.echostreams.pulsar.jms.queue;

import com.echostreams.pulsar.jms.config.PulsarConnectionFactory;

import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;

public class PulsarQueueConnectionFactory extends PulsarConnectionFactory implements QueueConnectionFactory {
    @Override
    public QueueConnection createQueueConnection() throws JMSException {
        return null;
    }

    @Override
    public QueueConnection createQueueConnection(String s, String s1) throws JMSException {
        return null;
    }
}
