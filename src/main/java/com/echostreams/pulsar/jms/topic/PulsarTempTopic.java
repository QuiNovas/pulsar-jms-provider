package com.echostreams.pulsar.jms.topic;

import javax.jms.JMSException;
import javax.jms.TemporaryTopic;

public class PulsarTempTopic extends PulsarTopic implements TemporaryTopic {
    @Override
    public void delete() throws JMSException {

    }
}
