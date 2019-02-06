package com.echostreams.pulsar.jms.topic;

import com.echostreams.pulsar.jms.common.AbstractDestination;

import javax.jms.JMSException;
import javax.jms.Topic;

public class PulsarTopic extends AbstractDestination implements Topic {
    @Override
    public String getTopicName() throws JMSException {
        return null;
    }
}
