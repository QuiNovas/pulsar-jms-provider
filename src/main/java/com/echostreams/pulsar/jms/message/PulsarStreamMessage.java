package com.echostreams.pulsar.jms.message;

import com.echostreams.pulsar.jms.config.PulsarConfig;
import com.echostreams.pulsar.jms.utils.CommonUtils;
import com.echostreams.pulsar.jms.utils.MessageConverterUtils;
import com.echostreams.pulsar.jms.utils.MessageUtils;

import javax.jms.*;
import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class PulsarStreamMessage extends PulsarMessage implements StreamMessage {
    //private Stream payload;

    /**
     * Empty byte array for initialisation purposes.
     */
    private static final byte[] EMPTY = new byte[]{};
    private byte[] payload = EMPTY;

    /**
     * list holding the message body
     */
    private ArrayList body;

    /**
     * the current read position
     */
    private int pos;

    /**
     * InputStream used to read the current byte[]
     */
    private ByteArrayInputStream byteStream;

    public PulsarStreamMessage() throws JMSException {
        headers = new HashMap<>();
        headers.put(PROPERTIES, new HashMap<String, Serializable>());
        setJMSType(PulsarConfig.STREAM_MESSAGE);
        setToWriteOnlyMode();
    }

    /* (non-Javadoc)
     * @see javax.jms.Message#clearBody()
     */
    @Override
    public void clearBody() throws JMSException {
        payload = EMPTY;
        body.clear();
        pos = 0;
    }

    /* (non-Javadoc)
     * @see javax.jms.Message#getBody(java.lang.Class)
     */
    @Override
    public <T> T getBody(Class<T> c) throws JMSException {
        return (T) payload;
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#readBoolean()
     */
    @Override
    public boolean readBoolean() throws JMSException {
        return MessageConverterUtils.convertToBoolean(readInternal());
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#readByte()
     */
    @Override
    public byte readByte() throws JMSException {
        return MessageConverterUtils.convertToByte(readInternal());
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#readShort()
     */
    @Override
    public short readShort() throws JMSException {
        return MessageConverterUtils.convertToShort(readInternal());
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#readChar()
     */
    @Override
    public char readChar() throws JMSException {
        return MessageConverterUtils.convertToChar(readInternal());
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#readInt()
     */
    @Override
    public int readInt() throws JMSException {
        return MessageConverterUtils.convertToInt(readInternal());
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#readLong()
     */
    @Override
    public long readLong() throws JMSException {
        return MessageConverterUtils.convertToLong(readInternal());
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#readFloat()
     */
    @Override
    public float readFloat() throws JMSException {
        return MessageConverterUtils.convertToFloat(readInternal());
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#readDouble()
     */
    @Override
    public double readDouble() throws JMSException {
        return MessageConverterUtils.convertToDouble(readInternal());
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#readString()
     */
    @Override
    public String readString() throws JMSException {
        return MessageConverterUtils.convertToString(readInternal());
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#readBytes(byte[])
     */
    @Override
    public int readBytes(byte[] value) throws JMSException {
        //first call to readBytes
        if (this.payload == null && this.byteStream == null) {
            this.payload = MessageConverterUtils.convertToBytes(readInternal());
            this.byteStream = new ByteArrayInputStream(this.payload);
        }
        //read
        int count = this.byteStream.read(value, 0, value.length);
        //byte[] completely read but maybe no eof (-1) yet
        if (count < value.length) {
            this.payload = null;
        }
        //eof - reset the stream
        if (count < 0) {
            this.byteStream = null;
        }

        return count;
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#readObject()
     */
    @Override
    public Object readObject() throws JMSException {
        Object value = readInternal();
        if (value == null) {
            throw new NullPointerException("null is not allowed.");
        }
        if (MessageUtils.isValidType(value) == false) {
            throw new MessageFormatException("invalid type");
        }
        return value;
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#writeBoolean(boolean)
     */
    @Override
    public void writeBoolean(boolean value) throws JMSException {
        writeInternal(new Boolean(value));
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#writeByte(byte)
     */
    @Override
    public void writeByte(byte value) throws JMSException {
        writeInternal(new Byte(value));
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#writeShort(short)
     */
    @Override
    public void writeShort(short value) throws JMSException {
        writeInternal(new Short(value));
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#writeChar(char)
     */
    @Override
    public void writeChar(char value) throws JMSException {
        writeInternal(new Character(value));
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#writeInt(int)
     */
    @Override
    public void writeInt(int value) throws JMSException {
        writeInternal(new Integer(value));
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#writeLong(long)
     */
    @Override
    public void writeLong(long value) throws JMSException {
        writeInternal(new Long(value));
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#writeFloat(float)
     */
    @Override
    public void writeFloat(float value) throws JMSException {
        writeInternal(new Float(value));
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#writeDouble(double)
     */
    @Override
    public void writeDouble(double value) throws JMSException {
        writeInternal(new Double(value));
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#writeString(java.lang.String)
     */
    @Override
    public void writeString(String value) throws JMSException {
        writeInternal(value);
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#writeBytes(byte[])
     */
    @Override
    public void writeBytes(byte[] value) throws JMSException {
        byte[] bytes = null;
        //copy array
        if (value != null) {
            CommonUtils.copy(value);
        }

        //set value
        writeInternal(bytes);
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#writeBytes(byte[], int, int)
     */
    @Override
    public void writeBytes(byte[] value, int offset, int length) throws JMSException {
        byte[] bytes = null;
        //copy array
        if (value != null) {
            bytes = new byte[length];
            System.arraycopy(value, offset, bytes, 0, length);
        }
        //set value
        writeInternal(bytes);
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#writeObject(java.lang.Object)
     */
    @Override
    public void writeObject(Object value) throws JMSException {
        checkWriteMode();
        if (MessageUtils.isValidType(value) == false) {
            throw new MessageFormatException("invalid type");
        }
        writeInternal(value);
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#reset()
     */
    @Override
    public void reset() throws JMSException {
        //set read-only mode
        this.readOnlyBody = true;
        this.pos = 0;
    }

    private void setToWriteOnlyMode() {
        this.body = new ArrayList();
        this.readOnlyBody = false;
    }

    private Object readInternal()
            throws MessageNotReadableException, MessageEOFException, MessageFormatException {
        //read mode?
        checkReadMode();
        //pending byte[] field?
        if (this.payload != null) {
            throw new MessageFormatException("There is a pending read of a byte[] field.");
        }
        //another field available?
        if (this.pos >= this.body.size()) {
            throw new MessageEOFException("Trying to read entry at position "
                    + this.pos +
                    " while only " +
                    this.body.size() +
                    " entries are available");
        }
        Object value = this.body.get(this.pos);
        this.pos++;
        return value;
    }

    private void writeInternal(Object value) throws MessageNotWriteableException {
        checkWriteMode();
        this.body.add(value);
    }

}
