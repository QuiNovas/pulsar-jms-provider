package com.echostreams.pulsar.jms.common.destination;

public interface DestinationDescriptorMBean {
    String getName();

    boolean isTemporary();

    int getInitialBlockCount();

    int getMaxNonPersistentMessages();
}
