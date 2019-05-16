package com.echostreams.pulsar.jms.message;

import com.echostreams.pulsar.jms.config.PulsarConstants;
import com.echostreams.pulsar.jms.utils.CommonUtils;
import com.echostreams.pulsar.jms.utils.MessageConverterUtils;
import com.echostreams.pulsar.jms.utils.MessageUtils;

import javax.jms.*;
import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class PulsarStreamMessage extends PulsarMessage implements StreamMessage {

    //private Stream payload;
    private static final long serialVersionUID = 1365492956623963016L;
    private byte[] payload;

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
        super();
        headers = new HashMap<>();
        headers.put(PROPERTIES, new HashMap<String, Serializable>());
        setJMSType(PulsarConstants.STREAM_MESSAGE);
        setToWriteOnlyMode();
    }

    @Override
    public void clearBody() throws JMSException {
        this.payload = new byte[0];
        body.clear();
        pos = 0;
        this.readOnlyBody = false;
    }

    @Override
    public <T> T getBody(Class<T> c) throws JMSException {
        return (T) body;
    }

    @Override
    public boolean readBoolean() throws JMSException {
        return MessageConverterUtils.convertToBoolean(readInternal());
    }

    @Override
    public byte readByte() throws JMSException {
        return MessageConverterUtils.convertToByte(readInternal());
    }

    @Override
    public short readShort() throws JMSException {
        return MessageConverterUtils.convertToShort(readInternal());
    }

    @Override
    public char readChar() throws JMSException {
        return MessageConverterUtils.convertToChar(readInternal());
    }

    @Override
    public int readInt() throws JMSException {
        return MessageConverterUtils.convertToInt(readInternal());
    }

    @Override
    public long readLong() throws JMSException {
        return MessageConverterUtils.convertToLong(readInternal());
    }

    @Override
    public float readFloat() throws JMSException {
        return MessageConverterUtils.convertToFloat(readInternal());
    }

    @Override
    public double readDouble() throws JMSException {
        return MessageConverterUtils.convertToDouble(readInternal());
    }

    @Override
    public String readString() throws JMSException {
        return MessageConverterUtils.convertToString(readInternal());
    }

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

    @Override
    public void writeBoolean(boolean value) throws JMSException {
        writeInternal(new Boolean(value));
    }

    @Override
    public void writeByte(byte value) throws JMSException {
        writeInternal(new Byte(value));
    }

    @Override
    public void writeShort(short value) throws JMSException {
        writeInternal(new Short(value));
    }

    @Override
    public void writeChar(char value) throws JMSException {
        writeInternal(new Character(value));
    }

    @Override
    public void writeInt(int value) throws JMSException {
        writeInternal(new Integer(value));
    }

    @Override
    public void writeLong(long value) throws JMSException {
        writeInternal(new Long(value));
    }

    @Override
    public void writeFloat(float value) throws JMSException {
        writeInternal(new Float(value));
    }

    @Override
    public void writeDouble(double value) throws JMSException {
        writeInternal(new Double(value));
    }

    @Override
    public void writeString(String value) throws JMSException {
        writeInternal(value);
    }

    @Override
    public void writeBytes(byte[] value) throws JMSException {
        byte[] bytes = null;
        //copy array
        if (value != null) {
            bytes = CommonUtils.copy(value);
        }

        //set value
        writeInternal(bytes);
    }

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

    @Override
    public void writeObject(Object value) throws JMSException {
        if (MessageUtils.isValidType(value) == false) {
            throw new MessageFormatException("invalid type");
        }
        writeInternal(value);
    }

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

    @Override
    public String toString() {
        return "PulsarStreamMessage{" +
                "payload=" + Arrays.toString(payload) +
                ", body=" + body +
                ", pos=" + pos +
                ", byteStream=" + byteStream +
                '}';
    }
}
