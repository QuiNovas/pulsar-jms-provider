package com.echostreams.pulsar.jms.common;

import javax.jms.*;

public abstract class AbstractConnection implements Connection {


    @Override
    public Session createSession(boolean b, int i) throws JMSException {
        return null;
    }

    @Override
    public String getClientID() throws JMSException {
        return null;
    }

    @Override
    public void setClientID(String s) throws JMSException {

    }

    @Override
    public ConnectionMetaData getMetaData() throws JMSException {
        return null;
    }

    @Override
    public ExceptionListener getExceptionListener() throws JMSException {
        return null;
    }

    @Override
    public void setExceptionListener(ExceptionListener exceptionListener) throws JMSException {

    }

    @Override
    public void start() throws JMSException {

    }

    @Override
    public void stop() throws JMSException {

    }

    @Override
    public void close() throws JMSException {

    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Destination destination, String s, ServerSessionPool serverSessionPool, int i) throws JMSException {
        return null;
    }

    @Override
    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String s, String s1, ServerSessionPool serverSessionPool, int i) throws JMSException {
        return null;
    }
}
