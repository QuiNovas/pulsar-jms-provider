package com.echostreams.pulsar.jms.message;

import com.echostreams.pulsar.jms.common.AbstractMessage;
import com.echostreams.pulsar.jms.common.AbstractMessageConsumer;
import com.echostreams.pulsar.jms.common.AbstractSession;
import com.echostreams.pulsar.jms.utils.id.IntegerID;

import javax.jms.Destination;
import javax.jms.JMSException;

public class PulsarMessageConsumer extends AbstractMessageConsumer {

    public PulsarMessageConsumer(AbstractSession session, Destination destination, String messageSelector, boolean noLocal, IntegerID consumerId) throws JMSException {
        super(session, destination, messageSelector, noLocal, consumerId);
    }

    @Override
    protected AbstractMessage receiveFromDestination(long timeout, boolean duplicateRequired) throws JMSException {
        return null;
    }
}
