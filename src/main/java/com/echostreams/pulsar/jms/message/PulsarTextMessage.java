package com.echostreams.pulsar.jms.message;

import com.echostreams.pulsar.jms.config.PulsarConfig;

import javax.jms.JMSException;
import javax.jms.TextMessage;
import java.io.Serializable;
import java.util.HashMap;

public class PulsarTextMessage extends PulsarMessage implements TextMessage {
    private String payload;

    public PulsarTextMessage() throws JMSException {
        headers = new HashMap<>();
        headers.put(PROPERTIES, new HashMap<String, Serializable>());
        setJMSType(PulsarConfig.TEXT_MESSAGE);
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
