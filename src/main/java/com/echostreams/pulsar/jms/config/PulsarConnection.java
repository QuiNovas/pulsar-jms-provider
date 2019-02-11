package com.echostreams.pulsar.jms.config;

import com.echostreams.pulsar.jms.PulsarJMSProvider;
import com.echostreams.pulsar.jms.common.AbstractConnection;
import com.echostreams.pulsar.jms.utils.PulsarJMSException;
import com.echostreams.pulsar.jms.utils.id.IntegerID;

import javax.jms.*;

public class PulsarConnection extends AbstractConnection {

    protected PulsarJMSProvider pulsarJMSProvider;

    public PulsarConnection(String clientID) {
        super(clientID);
    }

    @Override
    public void deleteTemporaryQueue(String queueName) throws JMSException {
        pulsarJMSProvider.deleteQueue(queueName);
        unregisterTemporaryQueue(queueName);
    }

    @Override
    public void deleteTemporaryTopic(String topicName) throws JMSException {
        pulsarJMSProvider.deleteTopic(topicName);
        unregisterTemporaryTopic(topicName);
    }

    @Override
    public Session createSession(boolean transacted, int acknowledgeMode) throws JMSException {
        return createSession(idProvider.createID(), transacted, acknowledgeMode);
    }

    public final Session createSession(IntegerID sessionId, boolean transacted, int acknowledgeMode) throws JMSException {
        if (!transacted && acknowledgeMode == Session.SESSION_TRANSACTED)
            throw new PulsarJMSException("Acknowledge mode SESSION_TRANSACTED cannot be used for an non-transacted session", "INVALID_ACK_MODE");

        externalAccessLock.readLock().lock();
        try {
            isClosed();

            PulsarSession session = new PulsarSession(sessionId, this, pulsarJMSProvider, transacted, acknowledgeMode);
            registerSession(session);
            return session;
        } finally {
            externalAccessLock.readLock().unlock();
        }
    }


    @Override
    public ConnectionMetaData getMetaData() throws JMSException {
        return null;
    }

    @Override
    public void start() throws JMSException {
        externalAccessLock.readLock().lock();
        try {
            isClosed();
            if (started)
                return;
            started = true;

            // Wake up waiting consumers
            wakeUpLocalConsumers();
        } finally {
            externalAccessLock.readLock().unlock();
        }
    }

    @Override
    public void stop() throws JMSException {
        externalAccessLock.readLock().lock();
        try {
            isClosed();
            if (!started)
                return;
            started = false;

            // Wait for running deliveries to complete ...
            waitForDeliverySync();
        } finally {
            externalAccessLock.readLock().unlock();
        }
    }
}
