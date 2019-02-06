package com.echostreams.pulsar.jms.topic;

import com.echostreams.pulsar.jms.config.PulsarConnection;

import javax.jms.*;

public class PulsarTopicConnection extends PulsarConnection implements TopicConnection {
    @Override
    public TopicSession createTopicSession(boolean b, int i) throws JMSException {
        return null;
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Topic topic, String s, ServerSessionPool serverSessionPool, int i) throws JMSException {
        return null;
    }
}
