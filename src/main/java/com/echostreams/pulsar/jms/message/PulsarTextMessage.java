package com.echostreams.pulsar.jms.message;

import com.echostreams.pulsar.jms.common.AbstractMessage;

import javax.jms.JMSException;
import javax.jms.TextMessage;

public class PulsarTextMessage extends AbstractMessage implements TextMessage {
    @Override
    public void setText(String s) throws JMSException {

    }

    @Override
    public String getText() throws JMSException {
        return null;
    }
}
