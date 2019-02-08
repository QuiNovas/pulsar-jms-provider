package com.echostreams.pulsar.jms.queue;

import com.echostreams.pulsar.jms.config.PulsarConnection;

import javax.jms.*;

public class PulsarQueueConnection extends PulsarConnection implements QueueConnection {

    public PulsarQueueConnection(String clientID) {
        super(clientID);
    }

    @Override
    public QueueSession createQueueSession(boolean b, int i) throws JMSException {
        return null;
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Queue queue, String s, ServerSessionPool serverSessionPool, int i) throws JMSException {
        return null;
    }
}
