package com.echostreams.pulsar.jms.message;

import com.echostreams.pulsar.jms.common.AbstractMessage;
import com.echostreams.pulsar.jms.common.AbstractMessageProducer;
import com.echostreams.pulsar.jms.common.MessageTools;
import com.echostreams.pulsar.jms.config.PulsarSession;
import com.echostreams.pulsar.jms.utils.id.IntegerID;

import javax.jms.Destination;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Message;

public class PulsarMessageProducer extends AbstractMessageProducer {

    public PulsarMessageProducer(PulsarSession session,Destination destination,IntegerID producerId) throws JMSException
    {
        super(session,destination,producerId);
        this.session = session;
    }

    @Override
    protected final void sendToDestination(Destination destination, boolean destinationOverride, Message srcMessage, int deliveryMode, int priority, long timeToLive) throws JMSException
    {
        // Check that the destination was specified
        if (destination == null)
            throw new InvalidDestinationException("Destination not specified");  // [JMS SPEC]

        // Create an internal copy if necessary
        AbstractMessage message = MessageTools.makeInternalCopy(srcMessage);

        externalAccessLock.readLock().lock();
        try
        {
            checkNotClosed();

            // Dispatch to session
            ((PulsarSession)session).dispatch(message);
        }
        finally
        {
            externalAccessLock.readLock().unlock();
        }
    }
}
