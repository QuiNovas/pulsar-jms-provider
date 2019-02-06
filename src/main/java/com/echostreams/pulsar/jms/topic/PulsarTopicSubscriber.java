package com.echostreams.pulsar.jms.topic;

import com.echostreams.pulsar.jms.message.PulsarMessageConsumer;

import javax.jms.JMSException;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

public class PulsarTopicSubscriber extends PulsarMessageConsumer implements TopicSubscriber {
    @Override
    public Topic getTopic() throws JMSException {
        return null;
    }

    @Override
    public boolean getNoLocal() throws JMSException {
        return false;
    }
}
