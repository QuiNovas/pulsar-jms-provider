package com.echostreams.pulsar.jms.jmx;

import javax.jms.JMSException;
import javax.management.ObjectName;

public interface PulsarJMXAgent {

    void registerMBean(ObjectName name, Object mBean) throws JMSException;

    void unregisterMBean(ObjectName name) throws JMSException;

    void stop();


}
