package com.echostreams.pulsar.jms.message;

import javax.jms.JMSException;
import javax.jms.TextMessage;
import java.io.Serializable;
import java.util.HashMap;

public class PulsarTextMessage extends PulsarMessage implements TextMessage {
    private String payload;

    public PulsarTextMessage() {
        headers = new HashMap<>();
        headers.put(PROPERTIES, new HashMap<String, Serializable>());
    }

    /* (non-Javadoc)
     * @see javax.jms.Message#clearBody()
     */
    @Override
    public void clearBody() throws JMSException {
        payload = null;
    }

    /* (non-Javadoc)
     * @see javax.jms.Message#getBody(java.lang.Class)
     */
    @Override
    public <T> T getBody(Class<T> c) throws JMSException {
        return (T) payload;
    }

    /* (non-Javadoc)
     * @see javax.jms.TextMessage#setText(java.lang.String)
     */
    @Override
    public void setText(String string) throws JMSException {
        payload = string;
    }

    /* (non-Javadoc)
     * @see javax.jms.TextMessage#getText()
     */
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
