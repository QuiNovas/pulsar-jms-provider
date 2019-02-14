package com.echostreams.pulsar.jms.topic;

import com.echostreams.pulsar.jms.config.PulsarSession;
import com.echostreams.pulsar.jms.message.PulsarMessageProducer;
import com.echostreams.pulsar.jms.utils.id.IntegerID;

import javax.jms.*;

public class PulsarTopicPublisher extends PulsarMessageProducer implements TopicPublisher {

    public PulsarTopicPublisher(PulsarSession session, Destination destination, IntegerID publisherId) throws JMSException {
        super(session, destination, publisherId);
    }

    @Override
    public Topic getTopic() throws JMSException {
        return (Topic) destination;
    }

    @Override
    public void publish(Message message) throws JMSException {
        send(message);
    }

    @Override
    public void publish(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        send(message, deliveryMode, priority, timeToLive);
    }

    @Override
    public void publish(Topic topic, Message message) throws JMSException {
        send(topic, message);
    }

    @Override
    public void publish(Topic topic, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        send(topic, message, deliveryMode, priority, timeToLive);
    }
}
