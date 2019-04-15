package com.echostreams.pulsar.jms.client;

import com.echostreams.pulsar.jms.jmx.mbeans.PulsarQueueMXBean;

import javax.jms.JMSException;
import javax.jms.Queue;
import java.util.concurrent.atomic.AtomicLong;

public class PulsarQueue implements Queue, PulsarDestination, PulsarQueueMXBean {
    private static final long serialVersionUID = -7830091263426455391L;

    private String queueName;

    // Stats
    private AtomicLong totalSentMessageCount = new AtomicLong();
    private AtomicLong totalReceivedMessageCount = new AtomicLong();

    public PulsarQueue() {
    }

    public PulsarQueue(String queueName) {
        this.queueName = queueName;
    }

    @Override
    public String getQueueName() throws JMSException {
        return queueName;
    }

    @Override
    public String getName() throws JMSException {
        return getQueueName();
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
        return "PulsarQueue{" +
                "queueName='" + queueName + '\'' +
                ", totalSentMessageCount=" + totalSentMessageCount +
                ", totalReceivedMessageCount=" + totalReceivedMessageCount +
                '}';
    }
}
