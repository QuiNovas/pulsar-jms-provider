package com.echostreams.pulsar.jms.message;

import com.echostreams.pulsar.jms.common.AbstractMessage;
import com.echostreams.pulsar.jms.common.MessageGroup;
import com.echostreams.pulsar.jms.common.MessageSerializationLevel;
import com.echostreams.pulsar.jms.utils.PulsarJMSException;
import com.echostreams.pulsar.jms.utils.RawDataBuffer;

import javax.jms.*;
import java.io.*;

public class PulsarBytesMessage extends AbstractMessage implements BytesMessage {

    private byte[] body;
    private transient DataInputStream input;
    private transient DataOutputStream output;
    private transient ByteArrayInputStream inputBuf;
    private transient ByteArrayOutputStream outputBuf;

    public PulsarBytesMessage() {
        super();
    }

    @Override
    protected byte getType() {
        return MessageGroup.BYTES;
    }

    @Override
    public AbstractMessage copy() {
        PulsarBytesMessage clone = new PulsarBytesMessage();
        copyCommonFields(clone);
        tidyUp();
        clone.body = this.body;

        return clone;
    }

    @Override
    protected void serializeBodyTo(RawDataBuffer out) {

    }

    @Override
    protected void unserializeBodyFrom(RawDataBuffer in) {

    }

    @Override
    public long getBodyLength() throws JMSException {
        if (!bodyIsReadOnly)
            throw new MessageNotReadableException("Message body is write-only");

        return body != null ? body.length : 0;
    }

    @Override
    public boolean readBoolean() throws JMSException {
        backupState();
        try {
            return getInput().readBoolean();
        } catch (EOFException e) {
            restoreState();
            throw new MessageEOFException("End of body reached");
        } catch (IOException e) {
            restoreState();
            throw new PulsarJMSException("Cannot read message body", "IO_ERROR", e);
        } catch (RuntimeException e) {
            restoreState();
            throw e;
        }
    }

    @Override
    public byte readByte() throws JMSException {
        backupState();
        try {
            return getInput().readByte();
        } catch (EOFException e) {
            restoreState();
            throw new MessageEOFException("End of body reached");
        } catch (IOException e) {
            restoreState();
            throw new PulsarJMSException("Cannot read message body", "IO_ERROR", e);
        } catch (RuntimeException e) {
            restoreState();
            throw e;
        }
    }

    @Override
    public int readUnsignedByte() throws JMSException {
        backupState();
        try {
            return getInput().readUnsignedByte();
        } catch (EOFException e) {
            restoreState();
            throw new MessageEOFException("End of body reached");
        } catch (IOException e) {
            restoreState();
            throw new PulsarJMSException("Cannot read message body", "IO_ERROR", e);
        } catch (RuntimeException e) {
            restoreState();
            throw e;
        }
    }

    @Override
    public short readShort() throws JMSException {
        backupState();
        try {
            return getInput().readShort();
        } catch (EOFException e) {
            restoreState();
            throw new MessageEOFException("End of body reached");
        } catch (IOException e) {
            restoreState();
            throw new PulsarJMSException("Cannot read message body", "IO_ERROR", e);
        } catch (RuntimeException e) {
            restoreState();
            throw e;
        }
    }

    @Override
    public int readUnsignedShort() throws JMSException {
        backupState();
        try {
            return getInput().readUnsignedShort();
        } catch (EOFException e) {
            restoreState();
            throw new MessageEOFException("End of body reached");
        } catch (IOException e) {
            restoreState();
            throw new PulsarJMSException("Cannot read message body", "IO_ERROR", e);
        } catch (RuntimeException e) {
            restoreState();
            throw e;
        }
    }

    @Override
    public char readChar() throws JMSException {
        backupState();
        try {
            return getInput().readChar();
        } catch (EOFException e) {
            restoreState();
            throw new MessageEOFException("End of body reached");
        } catch (IOException e) {
            restoreState();
            throw new PulsarJMSException("Cannot read message body", "IO_ERROR", e);
        } catch (RuntimeException e) {
            restoreState();
            throw e;
        }
    }

    @Override
    public int readInt() throws JMSException {
        backupState();
        try {
            return getInput().readInt();
        } catch (EOFException e) {
            restoreState();
            throw new MessageEOFException("End of body reached");
        } catch (IOException e) {
            restoreState();
            throw new PulsarJMSException("Cannot read message body", "IO_ERROR", e);
        } catch (RuntimeException e) {
            restoreState();
            throw e;
        }
    }

    @Override
    public long readLong() throws JMSException {
        backupState();
        try {
            return getInput().readLong();
        } catch (EOFException e) {
            restoreState();
            throw new MessageEOFException("End of body reached");
        } catch (IOException e) {
            restoreState();
            throw new PulsarJMSException("Cannot read message body", "IO_ERROR", e);
        } catch (RuntimeException e) {
            restoreState();
            throw e;
        }
    }

    @Override
    public float readFloat() throws JMSException {
        backupState();
        try {
            return getInput().readFloat();
        } catch (EOFException e) {
            restoreState();
            throw new MessageEOFException("End of body reached");
        } catch (IOException e) {
            restoreState();
            throw new PulsarJMSException("Cannot read message body", "IO_ERROR", e);
        } catch (RuntimeException e) {
            restoreState();
            throw e;
        }
    }

    @Override
    public double readDouble() throws JMSException {
        backupState();
        try {
            return getInput().readDouble();
        } catch (EOFException e) {
            restoreState();
            throw new MessageEOFException("End of body reached");
        } catch (IOException e) {
            restoreState();
            throw new PulsarJMSException("Cannot read message body", "IO_ERROR", e);
        } catch (RuntimeException e) {
            restoreState();
            throw e;
        }
    }

    @Override
    public String readUTF() throws JMSException {
        backupState();
        try {
            return getInput().readUTF();
        } catch (EOFException e) {
            restoreState();
            throw new MessageEOFException("End of body reached");
        } catch (IOException e) {
            restoreState();
            throw new PulsarJMSException("Cannot read message body", "IO_ERROR", e);
        } catch (RuntimeException e) {
            restoreState();
            throw e;
        }
    }

    @Override
    public int readBytes(byte[] value) throws JMSException {
        backupState();
        try {
            return getInput().read(value);
        } catch (EOFException e) {
            restoreState();
            throw new MessageEOFException("End of body reached");
        } catch (IOException e) {
            restoreState();
            throw new PulsarJMSException("Cannot read message body", "IO_ERROR", e);
        } catch (RuntimeException e) {
            restoreState();
            throw e;
        }
    }

    @Override
    public int readBytes(byte[] value, int length) throws JMSException {
        backupState();
        try {
            return getInput().read(value, 0, length);
        } catch (EOFException e) {
            restoreState();
            throw new MessageEOFException("End of body reached");
        } catch (IOException e) {
            restoreState();
            throw new PulsarJMSException("Cannot read message body", "IO_ERROR", e);
        } catch (RuntimeException e) {
            restoreState();
            throw e;
        }
    }

    @Override
    public void writeBoolean(boolean value) throws JMSException {
        try {
            getOutput().writeBoolean(value);
        } catch (IOException e) {
            throw new PulsarJMSException("Cannot write message body", "IO_ERROR", e);
        }
    }

    @Override
    public void writeByte(byte value) throws JMSException {
        try {
            getOutput().writeByte(value);
        } catch (IOException e) {
            throw new PulsarJMSException("Cannot write message body", "IO_ERROR", e);
        }
    }

    @Override
    public void writeShort(short value) throws JMSException {
        try {
            getOutput().writeShort(value);
        } catch (IOException e) {
            throw new PulsarJMSException("Cannot write message body", "IO_ERROR", e);
        }
    }

    @Override
    public void writeChar(char value) throws JMSException {
        try {
            getOutput().writeChar(value);
        } catch (IOException e) {
            throw new PulsarJMSException("Cannot write message body", "IO_ERROR", e);
        }
    }

    @Override
    public void writeInt(int value) throws JMSException {
        try {
            getOutput().writeInt(value);
        } catch (IOException e) {
            throw new PulsarJMSException("Cannot write message body", "IO_ERROR", e);
        }
    }

    @Override
    public void writeLong(long value) throws JMSException {
        try {
            getOutput().writeLong(value);
        } catch (IOException e) {
            throw new PulsarJMSException("Cannot write message body", "IO_ERROR", e);
        }
    }

    @Override
    public void writeFloat(float value) throws JMSException {
        try {
            getOutput().writeFloat(value);
        } catch (IOException e) {
            throw new PulsarJMSException("Cannot write message body", "IO_ERROR", e);
        }
    }

    @Override
    public void writeDouble(double value) throws JMSException {
        try {
            getOutput().writeDouble(value);
        } catch (IOException e) {
            throw new PulsarJMSException("Cannot write message body", "IO_ERROR", e);
        }
    }

    @Override
    public void writeUTF(String value) throws JMSException {
        try {
            getOutput().writeUTF(value);
        } catch (IOException e) {
            throw new PulsarJMSException("Cannot write message body", "IO_ERROR", e);
        }
    }

    @Override
    public void writeBytes(byte[] value) throws JMSException {
        try {
            getOutput().write(value);
        } catch (IOException e) {
            throw new PulsarJMSException("Cannot write message body", "IO_ERROR", e);
        }
    }

    @Override
    public void writeBytes(byte[] value, int offset, int length) throws JMSException {
        try {
            getOutput().write(value, offset, length);
        } catch (IOException e) {
            throw new PulsarJMSException("Cannot write message body", "IO_ERROR", e);
        }
    }

    @Override
    public void writeObject(Object value) throws JMSException {
        if (value == null)
            throw new NullPointerException(); // [JMS Spec]

        if (value instanceof Boolean)
            writeBoolean(((Boolean) value).booleanValue());
        else if (value instanceof Byte)
            writeByte(((Byte) value).byteValue());
        else if (value instanceof Short)
            writeShort(((Short) value).shortValue());
        else if (value instanceof Integer)
            writeInt(((Integer) value).intValue());
        else if (value instanceof Long)
            writeLong(((Long) value).longValue());
        else if (value instanceof Float)
            writeFloat(((Float) value).floatValue());
        else if (value instanceof Double)
            writeDouble(((Double) value).doubleValue());
        else if (value instanceof String)
            writeUTF((String) value);
        else if (value instanceof byte[])
            writeBytes((byte[]) value);
        else
            throw new MessageFormatException("Unsupported property value type : " + value.getClass().getName());
    }

    @Override
    public void reset() throws JMSException {
        assertDeserializationLevel(MessageSerializationLevel.FULL);

        tidyUp();
        bodyIsReadOnly = true;
    }

    @Override
    public void clearBody() throws JMSException {
        assertDeserializationLevel(MessageSerializationLevel.FULL);
        body = null;
        input = null;
        output = null;
        inputBuf = null;
        outputBuf = null;
        bodyIsReadOnly = false;
    }

    private void backupState() {
        if (inputBuf != null)
            inputBuf.mark(-1);
    }

    private void restoreState() {
        if (inputBuf != null)
            inputBuf.reset();
    }

    private DataInputStream getInput() throws JMSException {
        if (!bodyIsReadOnly)
            throw new MessageNotReadableException("Message body is write-only");

        if (input == null) {
            inputBuf = new ByteArrayInputStream(body != null ? body : new byte[0]);
            input = new DataInputStream(inputBuf);
        }

        return input;
    }

    private DataOutputStream getOutput() throws JMSException {
        if (bodyIsReadOnly)
            throw new MessageNotWriteableException("Message body is read-only");

        if (output == null) {
            outputBuf = new ByteArrayOutputStream(1024);
            output = new DataOutputStream(outputBuf);
        }

        return output;
    }

    private void tidyUp() {
        if (outputBuf != null) {
            body = outputBuf.toByteArray();
            outputBuf = null;
        }
        output = null;
        inputBuf = null;
        input = null;
    }

}
