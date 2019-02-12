package com.echostreams.pulsar.jms.message;

import com.echostreams.pulsar.jms.common.AbstractMessage;
import com.echostreams.pulsar.jms.common.MessageGroup;
import com.echostreams.pulsar.jms.common.MessageSerializationLevel;
import com.echostreams.pulsar.jms.utils.PulsarJMSException;
import com.echostreams.pulsar.jms.utils.RawDataBuffer;
import com.echostreams.pulsar.jms.utils.SerializationRelatedUtils;

import javax.jms.JMSException;
import javax.jms.MessageNotWriteableException;
import javax.jms.ObjectMessage;
import java.io.Serializable;

public class PulsarObjectMessage extends AbstractMessage implements ObjectMessage {

    private byte[] body;

    public PulsarObjectMessage() {
        super();
    }

    public PulsarObjectMessage(Serializable object) throws JMSException {
        super();
        setObject(object);
    }

    @Override
    protected byte getType() {
        return MessageGroup.OBJECT;
    }

    @Override
    public AbstractMessage copy() {
        PulsarObjectMessage clone = new PulsarObjectMessage();
        copyCommonFields(clone);
        clone.body = this.body;

        return clone;
    }

    @Override
    protected void serializeBodyTo(RawDataBuffer out) {
        out.writeNullableByteArray(body);
    }

    @Override
    protected void unserializeBodyFrom(RawDataBuffer in) {
        body = in.readNullableByteArray();
    }

    @Override
    public void setObject(Serializable object) throws JMSException {
        if (bodyIsReadOnly)
            throw new MessageNotWriteableException("Message body is read-only");

        assertDeserializationLevel(MessageSerializationLevel.FULL);

        if (object == null) {
            body = null;
            return;
        }

        try {
            body = SerializationRelatedUtils.toByteArray(object);
        } catch (Exception e) {
            throw new PulsarJMSException("Cannot serialize object message body", "MESSAGE_ERROR", e);
        }
    }

    @Override
    public Serializable getObject() throws JMSException {
        return null;
    }

    @Override
    public void clearBody() throws JMSException {
        assertDeserializationLevel(MessageSerializationLevel.FULL);
        body = null;
        bodyIsReadOnly = false;
    }

    @Override
    public String toString() {
        return super.toString() + " bodySize=" + body.length;
    }
}
