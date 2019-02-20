package com.echostreams.pulsar.jms.utils;

import javax.jms.JMSException;

public interface Checkable {
    public void check() throws JMSException;
}
