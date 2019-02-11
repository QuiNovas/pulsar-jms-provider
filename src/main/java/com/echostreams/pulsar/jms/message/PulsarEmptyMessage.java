package com.echostreams.pulsar.jms.message;

import com.echostreams.pulsar.jms.common.AbstractMessage;
import com.echostreams.pulsar.jms.common.MessageGroup;
import com.echostreams.pulsar.jms.utils.RawDataBuffer;

import javax.jms.JMSException;

public class PulsarEmptyMessage extends AbstractMessage {

    public PulsarEmptyMessage()
    {
        super();
    }

    @Override
    protected byte getType() {
        return MessageGroup.EMPTY;
    }

    @Override
    public AbstractMessage copy()
    {
        PulsarEmptyMessage clone = new PulsarEmptyMessage();
        copyCommonFields(clone);
        return clone;
    }

    @Override
    protected void serializeBodyTo(RawDataBuffer out) {

    }

    @Override
    protected void unserializeBodyFrom(RawDataBuffer in) {

    }

    @Override
    public void clearBody() throws JMSException {

    }
}
