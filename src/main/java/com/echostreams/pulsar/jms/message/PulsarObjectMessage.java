package com.echostreams.pulsar.jms.message;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;
import java.io.Serializable;
import java.util.HashMap;

public class PulsarObjectMessage extends PulsarMessage implements ObjectMessage {
    private Object payload;

    /**
     *
     */
    public PulsarObjectMessage() {
        headers = new HashMap<>();
        headers.put(PROPERTIES, new HashMap<String, Serializable>());
    }

    /* (non-Javadoc)
     * @see javax.jms.Message#clearBody()
     */
    @Override
    public void clearBody() throws JMSException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see javax.jms.Message#getBody(java.lang.Class)
     */
    @Override
    public <T> T getBody(Class<T> c) throws JMSException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.jms.ObjectMessage#setObject(java.io.Serializable)
     */
    @Override
    public void setObject(Serializable object) throws JMSException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see javax.jms.ObjectMessage#getObject()
     */
    @Override
    public Serializable getObject() throws JMSException {
        // TODO Auto-generated method stub
        return null;
    }

}
