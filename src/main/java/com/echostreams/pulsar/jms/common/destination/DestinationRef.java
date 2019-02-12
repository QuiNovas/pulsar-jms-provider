package com.echostreams.pulsar.jms.common.destination;

import javax.jms.Destination;
import javax.naming.Referenceable;
import java.io.Serializable;

public abstract class DestinationRef implements Destination, Serializable, Referenceable {
    private static final long serialVersionUID = 1L;

    public abstract String getResourceName();
}
