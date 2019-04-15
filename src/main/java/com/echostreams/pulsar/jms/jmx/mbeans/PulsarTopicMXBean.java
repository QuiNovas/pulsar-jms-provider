package com.echostreams.pulsar.jms.jmx.mbeans;

public interface PulsarTopicMXBean {

    void resetStats();

    long getTotalSentMessagesCount();

    void setTotalSentMessagesCount();

    long getTotalReceivedMessagesCount();

    void setTotalReceivedMessagesCount();
}
