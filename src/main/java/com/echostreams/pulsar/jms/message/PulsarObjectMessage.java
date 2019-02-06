package com.echostreams.pulsar.jms.message;

import com.echostreams.pulsar.jms.common.AbstractMessage;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;
import java.io.Serializable;

public class PulsarObjectMessage extends AbstractMessage implements ObjectMessage {

    @Override
    public void setObject(Serializable serializable) throws JMSException {

    }

    @Override
    public Serializable getObject() throws JMSException {
        return null;
    }
}
