package com.echostreams.pulsar.jms.jmx.local;

import com.echostreams.pulsar.jms.config.PulsarConstants;
import com.echostreams.pulsar.jms.jmx.AbstractPulsarJMXAgent;

import javax.management.MBeanServer;

public class PulsarLocalJMXAgent extends AbstractPulsarJMXAgent {

    public PulsarLocalJMXAgent(MBeanServer mBeanServer) {
        super(mBeanServer);
    }

    @Override
    protected String getType() {
        return PulsarConstants.JMX_LOCAL;
    }
}
