package com.echostreams.pulsar.jms.message;

import com.echostreams.pulsar.jms.common.AbstractMessage;
import com.echostreams.pulsar.jms.common.MessageConverter;
import com.echostreams.pulsar.jms.common.MessageGroup;
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
    public void writeShort(short value) throws JMSException {
        write(Short.valueOf(value));
    }

    @Override
    public void writeChar(char value) throws JMSException {
        write(new Character(value));
    }

    @Override
    public void writeInt(int value) throws JMSException {
        write(Integer.valueOf(value));
    }

    @Override
    public void writeLong(long value) throws JMSException {
        write(Long.valueOf(value));
    }

    @Override
    public void writeFloat(float value) throws JMSException {
        write(new Float(value));
    }

    @Override
    public void writeDouble(double value) throws JMSException {
        write(new Double(value));
    }

    @Override
    public void writeString(String value) throws JMSException {
        write(value);
    }

    @Override
    public void writeBytes(byte[] value) throws JMSException {
        write(value.clone());
    }

    @Override
    public void writeBytes(byte[] value, int offset, int length) throws JMSException {
        byte[] reducedValue = new byte[length];
        System.arraycopy(value, offset, reducedValue, 0, length);
        write(reducedValue);
    }

    @Override
    public void writeObject(Object value) throws JMSException {
        if (value != null) {
            // Check supported types
            if (!(value instanceof Boolean ||
                    value instanceof Byte ||
                    value instanceof Character ||
                    value instanceof Short ||
                    value instanceof Integer ||
                    value instanceof Long ||
                    value instanceof Float ||
                    value instanceof Double ||
                    value instanceof String ||
                    value instanceof byte[]))
                throw new MessageFormatException("Unsupported value type : " + value.getClass().getName());
        }

        write(value);
    }


    @Override
    public void reset() {
        bodyIsReadOnly = true;
        readPos = 0;
        currentByteInputStream = null;
    }

    @Override
    protected byte getType() {
        return MessageGroup.STREAM;
    }

    @Override
    public AbstractMessage copy() {
        PulsarStreamMessage clone = new PulsarStreamMessage();
        copyCommonFields(clone);
        @SuppressWarnings("unchecked")
        Vector<Object> bodyClone = (Vector<Object>) this.body.clone();
        clone.body = bodyClone;

        return clone;
    }

    @Override
    protected void serializeBodyTo(RawDataBuffer out) {
        out.writeInt(body.size());
        for (int n = 0; n < body.size(); n++) {
            Object value = body.get(n);
            out.writeGeneric(value);
        }
    }

    @Override
    protected void unserializeBodyFrom(RawDataBuffer in) {
        int size = in.readInt();
        body.ensureCapacity(size);
        for (int n = 0; n < size; n++) {
            Object value = in.readGeneric();
            body.add(value);
        }
    }

    @Override
    public void clearBody() throws JMSException {
        assertDeserializationLevel(MessageSerializationLevel.FULL);
        bodyIsReadOnly = false;
        body.clear();
        readPos = 0;
        currentByteInputStream = null;
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
