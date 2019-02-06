package com.echostreams.pulsar.jms.topic;

import com.echostreams.pulsar.jms.message.PulsarMessageProducer;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Topic;
import javax.jms.TopicPublisher;

public class PulsarTopicPublisher extends PulsarMessageProducer implements TopicPublisher {
    @Override
    public Topic getTopic() throws JMSException {
        return null;
    }

    @Override
    public void publish(Message message) throws JMSException {

    }

    @Override
    public void publish(Message message, int i, int i1, long l) throws JMSException {

    }

    @Override
    public void publish(Topic topic, Message message) throws JMSException {

    }

    @Override
    public void publish(Topic topic, Message message, int i, int i1, long l) throws JMSException {

    }
}
