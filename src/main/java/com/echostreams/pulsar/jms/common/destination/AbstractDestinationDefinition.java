package com.echostreams.pulsar.jms.common.destination;

import com.echostreams.pulsar.jms.utils.Settings;

import javax.jms.Destination;

/**
 * <p>Base implementation for a {@link Destination} definition descriptor.</p>
 */
public abstract class AbstractDestinationDefinition extends AbstractDestinationDescriptor {
    public AbstractDestinationDefinition() {
        super();
    }

    public AbstractDestinationDefinition(Settings settings) {
        super(settings);
    }
}
