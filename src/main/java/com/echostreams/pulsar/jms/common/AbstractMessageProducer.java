package com.echostreams.pulsar.jms.common;

import com.echostreams.pulsar.jms.utils.PulsarJMSException;
import com.echostreams.pulsar.jms.utils.id.IntegerID;
import com.echostreams.pulsar.jms.utils.id.UUIDProvider;

import javax.jms.*;

public abstract class AbstractMessageProducer extends AbstractMessageHandler implements MessageProducer {

    // Settings
    protected int defaultDeliveryMode = Message.DEFAULT_DELIVERY_MODE;
    protected int defaultPriority = Message.DEFAULT_PRIORITY;
    protected long defaultTimeToLive = Message.DEFAULT_TIME_TO_LIVE;
    protected boolean disableMessageID;
    protected boolean disableMessageTimestamp;

    // Utils
    protected UUIDProvider uuidProvider = UUIDProvider.getInstance();

    public AbstractMessageProducer(AbstractSession session, Destination destination, IntegerID producerId) {
        super(session, destination, producerId);
    }

    @Override
    public void setDisableMessageID(boolean disableMessageID) throws JMSException {
        this.disableMessageID = disableMessageID;
    }

    @Override
    public boolean getDisableMessageID() throws JMSException {
        return disableMessageID;
    }

    @Override
    public void setDisableMessageTimestamp(boolean disableMessageTimestamp) throws JMSException {
        this.disableMessageTimestamp = disableMessageTimestamp;
    }

    @Override
    public boolean getDisableMessageTimestamp() throws JMSException {
        return disableMessageTimestamp;
    }

    @Override
    public void setDeliveryMode(int deliveryMode) throws JMSException {
        if (deliveryMode != DeliveryMode.PERSISTENT &&
                deliveryMode != DeliveryMode.NON_PERSISTENT)
            throw new PulsarJMSException("Invalid delivery mode : "+deliveryMode,"INVALID_DELIVERY_MODE");

        this.defaultDeliveryMode = deliveryMode;
    }

    @Override
    public int getDeliveryMode() throws JMSException {
        return defaultDeliveryMode;
    }

    @Override
    public void setPriority(int priority) throws JMSException {
        if (priority < 0 || priority > 9)
            throw new PulsarJMSException("Invalid priority value : "+priority,"INVALID_PRIORITY");

        this.defaultPriority = priority;
    }

    @Override
    public int getPriority() throws JMSException {
        return defaultPriority;
    }

    @Override
    public void setTimeToLive(long timeToLive) throws JMSException {
        this.defaultTimeToLive = timeToLive;
    }

    @Override
    public long getTimeToLive() throws JMSException {
        return defaultTimeToLive;
    }

    @Override
    public Destination getDestination() throws JMSException {
        return destination;
    }

    @Override
    public void close() throws JMSException {
        externalAccessLock.readLock().lock();
        try
        {
            if (closed)
                return;
            closed = true;
            onProducerClose();
        }
        finally
        {
            externalAccessLock.readLock().unlock();
        }
    }

    @Override
    public void send(Message message) throws JMSException {
        if (this.destination == null)
            throw new UnsupportedOperationException("Destination was not set at creation time");

        // Setup message fields
        setupMessage(destination,message,defaultDeliveryMode,defaultPriority,defaultTimeToLive);

        // Handle foreign message implementations
        message = MessageTools.normalize(message);

        sendToDestination(destination,false,message,defaultDeliveryMode,defaultPriority,defaultTimeToLive);
    }
    @Override
    public void send(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        if (this.destination == null)
            throw new UnsupportedOperationException("Destination was not set at creation time");

        // Setup message fields
        setupMessage(destination,message,deliveryMode,priority,timeToLive);

        // Handle foreign message implementations
        message = MessageTools.normalize(message);

        sendToDestination(destination,false,message,deliveryMode,priority,timeToLive);
    }

    @Override
    public void send(Destination destination, Message message) throws JMSException {
        if (this.destination != null)
            throw new UnsupportedOperationException("Destination was set at creation time");

        // Setup message fields
        setupMessage(destination,message,defaultDeliveryMode,defaultPriority,defaultTimeToLive);

        // Handle foreign message implementations
        message = MessageTools.normalize(message);

        sendToDestination(destination,true,message,defaultDeliveryMode,defaultPriority,defaultTimeToLive);
    }

    @Override
    public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException
    {
        if (this.destination != null)
            throw new UnsupportedOperationException("Destination was set at creation time");

        // Setup message fields
        setupMessage(destination,message,deliveryMode,priority,timeToLive);

        // Handle foreign message implementations
        message = MessageTools.normalize(message);

        sendToDestination(destination,true,message,deliveryMode,priority,timeToLive);
    }

    protected final void onProducerClose()
    {
        session.unregisterProducer(this);
    }

    protected final void setupMessage( Destination destinationRef , Message message , int deliveryMode , int priority , long timeToLive) throws JMSException
    {
        long now = System.currentTimeMillis();

        // Setup headers
        message.setJMSMessageID(uuidProvider.getUUID());
        message.setJMSTimestamp(disableMessageTimestamp ? 0 : now);
        message.setJMSDeliveryMode(deliveryMode);
        message.setJMSPriority(priority);
        message.setJMSExpiration(timeToLive > 0 ? timeToLive+now : 0);
        message.setJMSDestination(destinationRef);
    }

    protected abstract void sendToDestination(Destination destination, boolean destinationOverride, Message srcMessage, int deliveryMode, int priority, long timeToLive) throws JMSException;
}
