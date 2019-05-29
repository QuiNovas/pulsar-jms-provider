package com.echostreams.pulsar.jms.jmx;

import javax.jms.JMSException;
import javax.management.ObjectName;

public interface PulsarJMXAgent {

    void registerMBean(Object mBean, ObjectName name) throws JMSException;

    void unregisterMBean(ObjectName name) throws JMSException;

    void stop();


}
