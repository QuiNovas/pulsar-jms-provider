package com.echostreams.pulsar.jms.client;

import com.echostreams.pulsar.jms.jmx.mbeans.PulsarTopicMXBean;

import javax.jms.JMSException;
import javax.jms.Topic;
import java.util.concurrent.atomic.AtomicLong;

public class PulsarTopic implements Topic, PulsarDestination, PulsarTopicMXBean {

    private static final long serialVersionUID = 4029670726631550860L;

    private String topicName;

    // Stats
    private AtomicLong totalSentMessageCount = new AtomicLong();
    private AtomicLong totalReceivedMessageCount = new AtomicLong();

    public PulsarTopic() {
    }

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

    @Override
    public String getTopicName() throws JMSException {
        return topicName;
    }

    @Override
    public String getName() throws JMSException {
        return getTopicName();
    }

    @Override
    public void resetStats() {
        totalSentMessageCount.set(0);
        totalReceivedMessageCount.set(0);
    }

    @Override
    public long getTotalSentMessagesCount() {
        return totalSentMessageCount.get();
    }

    @Override
    public void setTotalSentMessagesCount() {
        totalSentMessageCount.incrementAndGet();
    }

    @Override
    public long getTotalReceivedMessagesCount() {
        return totalReceivedMessageCount.get();
    }

    @Override
    public void setTotalReceivedMessagesCount() {
        totalReceivedMessageCount.incrementAndGet();
    }

    @Override
    public String toString() {
        return "PulsarTopic{" +
                "topicName='" + topicName + '\'' +
                ", totalSentMessageCount=" + totalSentMessageCount +
                ", totalReceivedMessageCount=" + totalReceivedMessageCount +
                '}';
    }
}
