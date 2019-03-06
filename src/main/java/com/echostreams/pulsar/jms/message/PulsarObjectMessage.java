package com.echostreams.pulsar.jms.message;

import com.echostreams.pulsar.jms.config.PulsarConfig;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;
import java.io.Serializable;
import java.util.HashMap;

public class PulsarObjectMessage extends PulsarMessage implements ObjectMessage {
    private Serializable payload;

    public PulsarObjectMessage() throws JMSException {
        headers = new HashMap<>();
        headers.put(PROPERTIES, new HashMap<String, Serializable>());
        setJMSType(PulsarConfig.OBJECT_MESSAGE);
    }

    @Override
    public void clearBody() throws JMSException {
        payload = null;
    }

    @Override
    public <T> T getBody(Class<T> c) throws JMSException {
        return (T) payload;
    }

    @Override
    public void setObject(Serializable object) throws JMSException {
        checkWriteMode();
        if (object == null)
            throw new IllegalArgumentException("Serializable object is null!");

        this.payload = object;
    }

    @Override
    public Serializable getObject() throws JMSException {
        return this.payload;
    }

}
