package com.echostreams.pulsar.jms.common;

import com.echostreams.pulsar.jms.utils.PulsarJMSException;
import com.echostreams.pulsar.jms.utils.id.IntegerID;

import javax.jms.*;
import javax.jms.IllegalStateException;

public abstract class AbstractMessageConsumer extends AbstractMessageHandler implements MessageConsumer {

    // Attributes
    protected String messageSelector;
    protected boolean noLocal;
    protected MessageListener messageListener;
    protected boolean autoAcknowledge;

    public AbstractMessageConsumer(AbstractSession session,
                                   Destination destination,
                                   String messageSelector,
                                   boolean noLocal,
                                   IntegerID consumerId) throws JMSException {
        super(session, destination, consumerId);
        this.messageSelector = messageSelector;
        this.noLocal = noLocal;
        this.autoAcknowledge =
                (session.getAcknowledgeMode() == Session.AUTO_ACKNOWLEDGE ||
                        session.getAcknowledgeMode() == Session.DUPS_OK_ACKNOWLEDGE);

        if (destination == null)
            throw new PulsarJMSException("Message consumer destination cannot be null", "INVALID_DESTINATION");
    }


    @Override
    public String getMessageSelector() throws JMSException {
        return null;
    }

    @Override
    public MessageListener getMessageListener() throws JMSException {
        return null;
    }

    @Override
    public void setMessageListener(MessageListener messageListener) throws JMSException {
        externalAccessLock.readLock().lock();
        try {
            checkNotClosed();
            this.messageListener = messageListener;
        } finally {
            externalAccessLock.readLock().unlock();
        }
    }

    @Override
    public Message receive() throws JMSException {
        return null;
    }

    @Override
    public Message receive(long timeout) throws JMSException {
        if (messageListener != null)
            throw new PulsarJMSException("Cannot receive messages while a listener is active", "INVALID_OPERATION");

        AbstractMessage message = receiveFromDestination(timeout, true);
        if (message != null) {
            message.ensureDeserializationLevel(MessageSerializationLevel.FULL);

            message.setSession(session);

            // Auto acknowledge message
            if (autoAcknowledge)
                session.acknowledge();
        }
        return message;
    }

    @Override
    public Message receiveNoWait() throws JMSException {
        return null;
    }

    @Override
    public void close() throws JMSException {
        externalAccessLock.writeLock().lock();
        try {
            if (closed)
                return;
            closed = true;
            onConsumerClose();
        } finally {
            externalAccessLock.writeLock().unlock();
        }
        onConsumerClosed();
    }

    protected void onConsumerClose() {
        session.unregisterConsumer(this);
    }

    protected void onConsumerClosed() {
        // Nothing
    }

    protected final void checkNotClosed() throws JMSException {
        if (closed)
            throw new IllegalStateException("Message handler is closed"); // [JMS SPEC]
    }

    protected abstract AbstractMessage receiveFromDestination(long timeout, boolean duplicateRequired) throws JMSException;


}
