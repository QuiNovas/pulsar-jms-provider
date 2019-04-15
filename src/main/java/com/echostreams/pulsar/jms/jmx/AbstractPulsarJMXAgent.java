package com.echostreams.pulsar.jms.jmx;

import com.echostreams.pulsar.jms.exceptions.PulsarJMSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

public abstract class AbstractPulsarJMXAgent implements PulsarJMXAgent {

    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractPulsarJMXAgent.class);

    protected MBeanServer mBeanServer; // Get the JVM MBean server

    public AbstractPulsarJMXAgent(MBeanServer mBeanServer) {
        this.mBeanServer = mBeanServer;
        init();
    }

    protected abstract String getType();

    private void init() {
        LOGGER.info("Starting JMX agent (" + getType() + ")");
    }

    @Override
    public void registerMBean(ObjectName name, Object mBean) throws JMSException {
        LOGGER.debug("Registering object " + name);
        try {
            if (name == null)
                throw new IllegalArgumentException("name may not be null!");
            if (mBean == null)
                throw new IllegalArgumentException("mBean may not be null!");
            this.mBeanServer.registerMBean(mBean, name);
        } catch (Exception e) {
            LOGGER.error("Cannot register MBean", "JMX_ERROR", e);
        }
    }

    @Override
    public void unregisterMBean(ObjectName name) throws JMSException {
        LOGGER.debug("Unregistering object " + name);
        try {
            if (name == null)
                throw new IllegalArgumentException("name may not be null!");
            this.mBeanServer.unregisterMBean(name);
        } catch (InstanceNotFoundException e) {
            throw new PulsarJMSException("Cannot unregister MBean", "InstanceNotFoundException", e);
        } catch (MBeanRegistrationException e) {
            throw new PulsarJMSException("Cannot unregister MBean", "MBeanRegistrationException", e);
        }
    }

    @Override
    public void stop() {

    }
}
