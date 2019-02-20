package com.echostreams.pulsar.jms.topic;

import com.echostreams.pulsar.jms.config.PulsarDestination;

import javax.jms.JMSException;
import javax.jms.Topic;

public class PulsarTopic extends PulsarDestination implements Topic {

    public PulsarTopic() {
    }

    public PulsarTopic(String name) {
        super(name);
    }

    @Override
    public String getTopicName() throws JMSException {
        return getPhysicalName();
    }
}
