package com.echostreams.pulsar.jms.message;

import com.echostreams.pulsar.jms.common.AbstractMessage;
import com.echostreams.pulsar.jms.common.MessageGroup;
import com.echostreams.pulsar.jms.common.MessageSerializationLevel;
import com.echostreams.pulsar.jms.utils.RawDataBuffer;

import javax.jms.JMSException;
import javax.jms.MessageNotWriteableException;
import javax.jms.TextMessage;

public class PulsarTextMessage extends AbstractMessage implements TextMessage {
    private String body;

    public PulsarTextMessage() {
        super();
    }

    public PulsarTextMessage( String text ) throws JMSException
    {
        super();
        setText(text);
    }

    @Override
    protected byte getType() {
        return MessageGroup.TEXT;
    }

    @Override
    public AbstractMessage copy() {
        PulsarTextMessage clone = new PulsarTextMessage();
        copyCommonFields(clone);
        clone.body = this.body;

        return clone;
    }

    @Override
    protected void serializeBodyTo(RawDataBuffer out) {
        out.writeNullableUTF(body);
    }

    @Override
    protected void unserializeBodyFrom(RawDataBuffer in) {
        body = in.readNullableUTF();
    }

    @Override
    public void setText(String body) throws JMSException {
        if (bodyIsReadOnly)
            throw new MessageNotWriteableException("Message body is read-only");

        assertDeserializationLevel(MessageSerializationLevel.FULL);

        this.body = body;
    }

    @Override
    public String getText() throws JMSException {
        return body;
    }

    @Override
    public void clearBody() throws JMSException {
        assertDeserializationLevel(MessageSerializationLevel.FULL);
        body = null;
        bodyIsReadOnly = false;
    }
}
