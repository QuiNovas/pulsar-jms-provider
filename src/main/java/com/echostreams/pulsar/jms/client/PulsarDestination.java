package com.echostreams.pulsar.jms.client;

import javax.jms.Destination;
import javax.jms.JMSException;
import java.io.Serializable;

public interface PulsarDestination extends Destination, Serializable {
    String getName() throws JMSException;
}
