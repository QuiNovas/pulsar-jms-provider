package com.echostreams.pulsar.jms;

import com.echostreams.pulsar.jms.client.PulsarDestination;

import javax.jms.JMSException;
import javax.jms.Topic;

public class PulsarTopic implements Topic, PulsarDestination {
    private String topicName;

    /**
     * @param topicName
     */
    public PulsarTopic(String topicName) {
        super();
        this.topicName = topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    /* (non-Javadoc)
     * @see javax.jms.Topic#getTopicName()
     */
    @Override
    public String getTopicName() throws JMSException {
        return topicName;
    }

    /* (non-Javadoc)
     * @see PulsarDestination#getName()
     */
    @Override
    public String getName() throws JMSException {
        return getTopicName();
    }

}
