package com.echostreams.pulsar.jms.topic;

import com.echostreams.pulsar.jms.config.PulsarSession;
import com.echostreams.pulsar.jms.message.PulsarMessageProducer;
import com.echostreams.pulsar.jms.utils.id.IntegerID;

import javax.jms.*;

public class PulsarTopicPublisher extends PulsarMessageProducer implements TopicPublisher {

    public PulsarTopicPublisher(PulsarSession session, Destination destination, IntegerID publisherId) throws JMSException
    {
        super(session, destination, publisherId);
    }



    @Override
    public Topic getTopic() throws JMSException {
        return (Topic)destination;
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

    @Override
    protected void sendToDestination(Destination destination, boolean destinationOverride, Message srcMessage, int deliveryMode, int priority, long timeToLive) throws JMSException {

    }
}
