package com.echostreams.pulsar.jms.queue;

import com.echostreams.pulsar.jms.PulsarJMSProvider;
import com.echostreams.pulsar.jms.config.PulsarConnection;
import com.echostreams.pulsar.jms.utils.PulsarJMSException;

import javax.jms.*;
import javax.jms.IllegalStateException;

public final class PulsarQueueConnection extends PulsarConnection implements QueueConnection {

    public PulsarQueueConnection(PulsarJMSProvider pulsarJMSProvider, String clientID) {
        super(pulsarJMSProvider, clientID);
    }

    @Override
    public QueueSession createQueueSession(boolean transacted, int acknowledgeMode) throws JMSException {
        isClosed();
        PulsarQueueSession session = new PulsarQueueSession(idProvider.createID(), this, pulsarJMSProvider, transacted, acknowledgeMode);
        registerSession(session);
        return session;
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Queue queue, String s, ServerSessionPool serverSessionPool, int i) throws JMSException {
        isClosed();
        throw new PulsarJMSException("Unsupported feature", "UNSUPPORTED_FEATURE");
    }

    @Override
    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        throw new IllegalStateException("Method not available on this domain.");
    }
}
