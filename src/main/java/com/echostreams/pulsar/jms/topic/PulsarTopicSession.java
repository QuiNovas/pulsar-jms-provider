package com.echostreams.pulsar.jms.topic;

import com.echostreams.pulsar.jms.PulsarJMSProvider;
import com.echostreams.pulsar.jms.common.AbstractSession;
import com.echostreams.pulsar.jms.utils.id.IntegerID;

import javax.jms.*;

public class PulsarTopicSession extends AbstractSession implements TopicSession {

    public PulsarTopicSession(IntegerID id, PulsarTopicConnection pulsarTopicConnection, PulsarJMSProvider pulsarJMSProvider, boolean transacted, int acknowledgeMode) {

    }

    @Override
    public TopicSubscriber createSubscriber(Topic topic) throws JMSException {
        return null;
    }

    @Override
    public TopicSubscriber createSubscriber(Topic topic, String s, boolean b) throws JMSException {
        return null;
    }

    @Override
    public TopicPublisher createPublisher(Topic topic) throws JMSException {
        return null;
    }
}
