package com.echostreams.pulsar.jms.message;

import com.echostreams.pulsar.jms.common.AbstractMessage;
import com.echostreams.pulsar.jms.common.MessageConverter;
import com.echostreams.pulsar.jms.common.MessageSerializationLevel;
import com.echostreams.pulsar.jms.utils.PulsarJMSException;
import com.echostreams.pulsar.jms.utils.RawDataBuffer;

import javax.jms.*;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Vector;

public class PulsarStreamMessage extends AbstractMessage implements StreamMessage {

    private Vector<Object> body = new Vector<>();
    private transient int readPos;
    private transient ByteArrayInputStream currentByteInputStream;

    // For rollback
    private transient int readPosBackup;
    private transient ByteArrayInputStream currentByteInputStreamBackup;

    /**
     * Constructor
     */
    public PulsarStreamMessage() {
        super();
    }

    @Override
    public boolean readBoolean() throws JMSException {
        backupState();
        try {
            return MessageConverter.asBoolean(internalReadObject());
        } catch (JMSException e) {
            restoreState();
            throw e;
        } catch (RuntimeException e) {
            restoreState();
            throw e;
        }
    }

    @Override
    public byte readByte() throws JMSException {
        backupState();
        try {
            return MessageConverter.asByte(internalReadObject());
        } catch (JMSException e) {
            restoreState();
            throw e;
        } catch (RuntimeException e) {
            restoreState();
            throw e;
        }
    }

    @Override
    public short readShort() throws JMSException {
        backupState();
        try {
            return MessageConverter.asShort(internalReadObject());
        } catch (JMSException e) {
            restoreState();
            throw e;
        } catch (RuntimeException e) {
            restoreState();
            throw e;
        }
    }

    @Override
    public char readChar() throws JMSException {
        backupState();
        try {
            return MessageConverter.asChar(internalReadObject());
        } catch (JMSException e) {
            restoreState();
            throw e;
        } catch (RuntimeException e) {
            restoreState();
            throw e;
        }
    }

    @Override
    public int readInt() throws JMSException {
        backupState();
        try {
            return MessageConverter.asInt(internalReadObject());
        } catch (JMSException e) {
            restoreState();
            throw e;
        } catch (RuntimeException e) {
            restoreState();
            throw e;
        }
    }

    @Override
    public long readLong() throws JMSException {
        backupState();
        try {
            return MessageConverter.asLong(internalReadObject());
        } catch (JMSException e) {
            restoreState();
            throw e;
        } catch (RuntimeException e) {
            restoreState();
            throw e;
        }
    }

    @Override
    public float readFloat() throws JMSException {
        backupState();
        try {
            return MessageConverter.asFloat(internalReadObject());
        } catch (JMSException e) {
            restoreState();
            throw e;
        } catch (RuntimeException e) {
            restoreState();
            throw e;
        }
    }

    @Override
    public double readDouble() throws JMSException {
        backupState();
        try {
            return MessageConverter.asDouble(internalReadObject());
        } catch (JMSException e) {
            restoreState();
            throw e;
        } catch (RuntimeException e) {
            restoreState();
            throw e;
        }
    }

    @Override
    public String readString() throws JMSException {
        backupState();
        try {
            return MessageConverter.asString(internalReadObject());
        } catch (JMSException e) {
            restoreState();
            throw e;
        } catch (RuntimeException e) {
            restoreState();
            throw e;
        }
    }

    @Override
    public int readBytes(byte[] value) throws JMSException {
        backupState();
        try {
            if (currentByteInputStream == null) {
                byte[] base = MessageConverter.asBytes(internalReadObject());
                if (base == null)
                    return -1; // [JMS Spec]
                currentByteInputStream = new ByteArrayInputStream(base);
            }

            try {
                int readAmount = currentByteInputStream.read(value);
                if (readAmount < value.length) {
                    currentByteInputStream = null; // end of stream reached
                }

                return readAmount;
            } catch (IOException e) {
                throw new PulsarJMSException("Cannot read stream message body", "IO_ERROR", e);
            }
        } catch (JMSException e) {
            restoreState();
            throw e;
        } catch (RuntimeException e) {
            restoreState();
            throw e;
        }
    }

    @Override
    public Object readObject() throws JMSException {
        return internalReadObject();
    }

    @Override
    public void writeBoolean(boolean value) throws JMSException {
        write(Boolean.valueOf(value));
    }

    @Override
    public void writeByte(byte value) throws JMSException {
        write(Byte.valueOf(value));
    }

    @Override
    public void writeShort(short i) throws JMSException {

    }

    @Override
    public void writeChar(char c) throws JMSException {

    }

    @Override
    public void writeInt(int i) throws JMSException {

    }

    @Override
    public void writeLong(long l) throws JMSException {

    }

    @Override
    public void writeFloat(float v) throws JMSException {

    }

    @Override
    public void writeDouble(double v) throws JMSException {

    }

    @Override
    public void writeString(String s) throws JMSException {

    }

    @Override
    public void writeBytes(byte[] bytes) throws JMSException {

    }

    @Override
    public void writeBytes(byte[] bytes, int i, int i1) throws JMSException {

    }

    @Override
    public void writeObject(Object o) throws JMSException {

    }

    @Override
    public void reset() throws JMSException {

    }

    @Override
    protected byte getType() {
        return 0;
    }

    @Override
    public AbstractMessage copy() {
        return null;
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

    private void backupState() {
        readPosBackup = readPos;
        currentByteInputStreamBackup = currentByteInputStream;
        if (currentByteInputStream != null)
            currentByteInputStream.mark(-1);
    }

    private void restoreState() {
        readPos = readPosBackup;
        currentByteInputStream = currentByteInputStreamBackup;
        if (currentByteInputStream != null)
            currentByteInputStream.reset();
    }

    public Object internalReadObject() throws JMSException {
        if (!bodyIsReadOnly)
            throw new MessageNotReadableException("Message body is write only");

        if (readPos >= body.size())
            throw new MessageEOFException("End of stream reached");

        if (currentByteInputStream != null)
            throw new MessageFormatException("Cannot read another object before the end of the byte array");

        return body.get(readPos++);
    }

    private void write(Object value) throws JMSException {
        assertDeserializationLevel(MessageSerializationLevel.FULL);
        if (bodyIsReadOnly)
            throw new MessageNotWriteableException("Message body is read-only");
        body.add(value);
    }

}
