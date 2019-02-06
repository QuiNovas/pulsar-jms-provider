package com.echostreams.pulsar.jms.topic;

import com.echostreams.pulsar.jms.common.AbstractSession;

import javax.jms.*;

public class PulsarTopicSession extends AbstractSession implements TopicSession {

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
