package com.echostreams.pulsar.jms.common;

import com.echostreams.pulsar.jms.utils.ErrorTools;
import com.echostreams.pulsar.jms.utils.PulsarJMSException;
import com.echostreams.pulsar.jms.utils.id.IntegerID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;

public abstract class AbstractMessageConsumer extends AbstractMessageHandler implements MessageConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMessageConsumer.class);

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
        return messageSelector;
    }

    @Override
    public MessageListener getMessageListener() throws JMSException {
        return messageListener;
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
        return receive(-1);
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
        return receive(0);
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

    protected abstract AbstractMessage receiveFromDestination(long timeout, boolean duplicateRequired) throws JMSException;

    public final void wakeUpMessageListener()
    {
        try
        {
            while (!closed)
            {
                synchronized (session.deliveryLock) // [JMS spec]
                {
                    AbstractMessage message = receiveFromDestination(0,true);
                    if (message == null)
                        break;

                    // Make sure the message is properly deserialized
                    message.ensureDeserializationLevel(MessageSerializationLevel.FULL);

                    // Make sure the message's session is set
                    message.setSession(session);

                    // Call the message listener
                    boolean listenerFailed = false;
                    try
                    {
                        messageListener.onMessage(message);
                    }
                    catch (Throwable e)
                    {
                        listenerFailed = true;
                        if (shouldLogListenersFailures())
                            LOGGER.error("Message listener failed",e);
                    }

                    // Auto acknowledge message
                    if (autoAcknowledge)
                    {
                        if (listenerFailed)
                            session.recover();
                        else
                            session.acknowledge();
                    }
                }
            }
        }
        catch (JMSException e)
        {
            ErrorTools.log(e, LOGGER);
        }
    }

    protected abstract boolean shouldLogListenersFailures();

    protected abstract void wakeUp() throws JMSException;

}
