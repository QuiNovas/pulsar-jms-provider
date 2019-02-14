package com.echostreams.pulsar.jms.topic;

import com.echostreams.pulsar.jms.PulsarJMSProvider;
import com.echostreams.pulsar.jms.config.PulsarSession;
import com.echostreams.pulsar.jms.message.PulsarMessageConsumer;
import com.echostreams.pulsar.jms.utils.id.IntegerID;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

public class PulsarTopicSubscriber extends PulsarMessageConsumer implements TopicSubscriber {

    public PulsarTopicSubscriber(PulsarJMSProvider pulsarJMSProvider, PulsarSession session, Destination destination, String messageSelector, boolean noLocal, IntegerID consumerId, String subscriberId) throws JMSException {
        super(pulsarJMSProvider, session, destination, messageSelector, noLocal, consumerId, subscriberId);
    }

    @Override
    public Topic getTopic() throws JMSException {
        return (Topic) destination;
    }
}
