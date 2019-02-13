package com.echostreams.pulsar.jms.topic;

import com.echostreams.pulsar.jms.PulsarJMSProvider;
import com.echostreams.pulsar.jms.config.PulsarSession;
import com.echostreams.pulsar.jms.utils.id.IntegerID;

import javax.jms.Destination;
import javax.jms.JMSException;

public final class PulsarDurableTopicSubscriber extends PulsarTopicSubscriber
{
    public PulsarDurableTopicSubscriber(PulsarJMSProvider pulsarJMSProvider, PulsarSession session, Destination destination, String messageSelector, boolean noLocal, IntegerID consumerId, String subscriberId) throws JMSException
    {
        super(pulsarJMSProvider,session,destination,messageSelector,noLocal,consumerId,subscriberId);
    }

    @Override
	public boolean isDurable()
    {
        return true;
    }
}
