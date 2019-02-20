package com.echostreams.pulsar.jms.common;

import com.echostreams.pulsar.jms.queue.PulsarQueue;

import javax.jms.JMSException;

/**
 * <p>Immutable message lock reference</p>
 */
public final class MessageLock {
    // Attributes
    private int handle;
    private int deliveryMode;
    private PulsarQueue destination;
    private AbstractMessage message;

    /**
     * Constructor
     */
    public MessageLock(int handle,
                       int deliveryMode,
                       PulsarQueue destination,
                       AbstractMessage message) {
        this.handle = handle;
        this.deliveryMode = deliveryMode;
        this.destination = destination;
        this.message = message;
    }

    public int getHandle() {
        return handle;
    }

    public int getDeliveryMode() {
        return deliveryMode;
    }

    public PulsarQueue getDestination() {
        return destination;
    }

    public AbstractMessage getMessage() {
        return message;
    }

    @Override
    public String toString() {
        StringBuilder sb = null;
        try {
            sb = new StringBuilder();
            sb.append("[MessageLock] handle=");
            sb.append(handle);
            sb.append(" destination=");
            sb.append(destination);
            sb.append(" deliveryMode=");
            sb.append(deliveryMode);
            sb.append(" msgId=");
            sb.append(message.getJMSMessageID());
        } catch (JMSException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    public TransactionItem toTransactionItem() throws JMSException {
        return new TransactionItem(handle, message.getJMSMessageID(), deliveryMode, destination);
    }
}
