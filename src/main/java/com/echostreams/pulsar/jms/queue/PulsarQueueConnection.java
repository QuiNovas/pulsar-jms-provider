package com.echostreams.pulsar.jms.queue;

import com.echostreams.pulsar.jms.config.PulsarConnection;
import com.echostreams.pulsar.jms.utils.PulsarJMSException;

import javax.jms.*;

public final class PulsarQueueConnection extends PulsarConnection implements QueueConnection {

    public PulsarQueueConnection(String clientID) {
        super(clientID);
    }

    @Override
    public QueueSession createQueueSession(boolean transacted, int acknowledgeMode) throws JMSException {
        isClosed();
        PulsarQueueSession session =  new PulsarQueueSession(idProvider.createID(),this,pulsarJMSProvider,transacted,acknowledgeMode);
        registerSession(session);
        return session;
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Queue queue, String s, ServerSessionPool serverSessionPool, int i) throws JMSException {
        isClosed();
        throw new PulsarJMSException("Unsupported feature","UNSUPPORTED_FEATURE");
    }
}
