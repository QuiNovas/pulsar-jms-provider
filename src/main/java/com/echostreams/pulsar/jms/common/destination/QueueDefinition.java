package com.echostreams.pulsar.jms.common.destination;

import com.echostreams.pulsar.jms.utils.Settings;

import javax.jms.JMSException;

public final class QueueDefinition extends AbstractDestinationDefinition {
    public QueueDefinition() {
        super();
    }

    public QueueDefinition(Settings settings) {
        super(settings);
    }

    @Override
    public void check() throws JMSException {
        super.check();

        // Check queue name
        DestinationTools.checkQueueName(name);
    }

    public boolean hasDescriptor() {
        return !isTemporary();
    }
}
