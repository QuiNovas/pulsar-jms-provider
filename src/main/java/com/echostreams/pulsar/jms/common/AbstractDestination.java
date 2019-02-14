package com.echostreams.pulsar.jms.common;

import com.echostreams.pulsar.jms.common.destination.PulsarDestinationMBean;
import com.echostreams.pulsar.jms.utils.Committable;

import javax.jms.Destination;

public abstract class AbstractDestination implements Destination, PulsarDestinationMBean, Committable {
}
