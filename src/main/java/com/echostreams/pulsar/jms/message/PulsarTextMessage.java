package com.echostreams.pulsar.jms.message;

import com.echostreams.pulsar.jms.config.PulsarConstants;

import javax.jms.JMSException;
import javax.jms.TextMessage;
import java.io.Serializable;
import java.util.HashMap;

public class PulsarTextMessage extends PulsarMessage implements TextMessage {

    private static final long serialVersionUID = 3988939655140380666L;

    private String payload;

    public PulsarTextMessage() throws JMSException {
        super();
        headers = new HashMap<>();
        headers.put(PROPERTIES, new HashMap<String, Serializable>());
        setJMSType(PulsarConstants.TEXT_MESSAGE);
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
    public void setText(String string) throws JMSException {
        payload = string;
    }

    @Override
    public String getText() throws JMSException {
        return payload;
    }

    @Override
    public String toString() {
        return "PulsarTextMessage{" +
                "payload='" + payload + '\'' +
                '}';
    }
}
