package com.echostreams.pulsar.jms.jmx.mbeans;

public interface PulsarQueueMXBean {

    void resetStats();

    long getTotalSentMessagesCount();

    void setTotalSentMessagesCount();

    long getTotalReceivedMessagesCount();

    void setTotalReceivedMessagesCount();
}
