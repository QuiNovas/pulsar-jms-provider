package com.echostreams.pulsar.jms.topic;

import com.echostreams.pulsar.jms.config.PulsarConnectionFactory;

import javax.jms.JMSException;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;

public class PulsarTopicConnectionFactory extends PulsarConnectionFactory implements TopicConnectionFactory {
    @Override
    public TopicConnection createTopicConnection() throws JMSException {
        return null;
    }

    @Override
    public TopicConnection createTopicConnection(String s, String s1) throws JMSException {
        return null;
    }
}
