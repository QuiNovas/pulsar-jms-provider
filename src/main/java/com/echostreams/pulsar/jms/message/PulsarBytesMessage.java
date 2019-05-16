package com.echostreams.pulsar.jms.message;

import com.echostreams.pulsar.jms.config.PulsarConstants;

import javax.jms.*;
import java.io.*;
import java.util.Arrays;
import java.util.HashMap;

public class PulsarBytesMessage extends PulsarMessage implements BytesMessage {

    private static final long serialVersionUID = -157624964729797793L;

    private byte[] payload;
    private transient DataInputStream input;
    private transient DataOutputStream output;
    private transient ByteArrayInputStream byteInput;
    private transient ByteArrayOutputStream byteOutput;

    public PulsarBytesMessage() throws JMSException {
        super();
        headers = new HashMap<>();
        headers.put(PROPERTIES, new HashMap<String, Serializable>());
        setJMSType(PulsarConstants.BYTES_MESSAGE);
        setToWriteOnlyMode();
    }

    @Override
    public void clearBody() throws JMSException {
        try {
            if (this.readOnlyBody) {
                // in read-only mode
                this.readOnlyBody = false;
                if (input != null) {
                    byteInput = null;
                    input.close();
                    input = null;
                }
            } else if (output != null) {
                // already in write-only mode
                byteOutput = null;
                output.close();
                output = null;
            }
            this.payload = new byte[0];
            byteOutput = null;
            output = null;
        } catch (IOException exception) {
            throw new JMSException(exception.getMessage());
        }

    }

    @Override
    public <T> T getBody(Class<T> c) throws JMSException {

        //message is in write-only mode
        if (!this.readOnlyBody) {
            //flush the outer DataOutputStream...
            try {
                output.flush();
            } catch (IOException e) {
                throw new JMSException(e.getMessage());
            }
            //and return the underlying stream content
            return (T) byteOutput.toByteArray();
        }
        //message is in read-only mode
        else {
            //return the complete content of the message
            return (T) this.payload;
        }
    }

    @Override
    public long getBodyLength() throws JMSException {
        checkReadMode();
        return payload != null ? payload.length : 0;
    }

    @Override
    public boolean readBoolean() throws JMSException {
        checkReadMode();
        try {
            return getInputStream().readBoolean();
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw new JMSException(e.getMessage());
        }
    }

    @Override
    public byte readByte() throws JMSException {
        checkReadMode();
        try {
            return getInputStream().readByte();
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw new JMSException(e.getMessage());
        }
    }

    @Override
    public int readUnsignedByte() throws JMSException {
        checkReadMode();
        try {
            return getInputStream().readUnsignedByte();
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw new JMSException(e.getMessage());
        }
    }

    @Override
    public short readShort() throws JMSException {
        checkReadMode();
        try {
            return getInputStream().readShort();
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw new JMSException(e.getMessage());
        }
    }

    @Override
    public int readUnsignedShort() throws JMSException {
        checkReadMode();
        try {
            return getInputStream().readUnsignedShort();
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw new JMSException(e.getMessage());
        }
    }

    @Override
    public char readChar() throws JMSException {
        checkReadMode();
        try {
            return getInputStream().readChar();
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw new JMSException(e.getMessage());
        }
    }

    @Override
    public int readInt() throws JMSException {
        checkReadMode();
        try {
            return getInputStream().readInt();
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw new JMSException(e.getMessage());
        }
    }

    @Override
    public long readLong() throws JMSException {
        checkReadMode();
        try {
            return getInputStream().readLong();
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw new JMSException(e.getMessage());
        }
    }

    @Override
    public float readFloat() throws JMSException {
        checkReadMode();
        try {
            return getInputStream().readFloat();
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw new JMSException(e.getMessage());
        }
    }

    @Override
    public double readDouble() throws JMSException {
        checkReadMode();
        try {
            return getInputStream().readDouble();
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw new JMSException(e.getMessage());
        }
    }

    @Override
    public String readUTF() throws JMSException {
        checkReadMode();
        try {
            return getInputStream().readUTF();
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw new JMSException(e.getMessage());
        }
    }

    @Override
    public int readBytes(byte[] value) throws JMSException {
        checkReadMode();
        try {
            return getInputStream().read(value);
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw new JMSException(e.getMessage());
        }
    }

    @Override
    public int readBytes(byte[] value, int length) throws JMSException {
        checkReadMode();
        try {
            return getInputStream().read(value, 0, length);
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw new JMSException(e.getMessage());
        }
    }

    @Override
    public void writeBoolean(boolean value) throws JMSException {
        checkWriteMode();
        try {
            getOutputStream().writeBoolean(value);
        } catch (IOException e) {
            throw new JMSException(e.getMessage());
        }
    }

    @Override
    public void writeByte(byte value) throws JMSException {
        checkWriteMode();
        try {
            getOutputStream().writeByte(value);
        } catch (IOException e) {
            throw new JMSException(e.getMessage());
        }
    }

    @Override
    public void writeShort(short value) throws JMSException {
        checkWriteMode();
        try {
            getOutputStream().writeShort(value);
        } catch (IOException e) {
            throw new JMSException(e.getMessage());
        }
    }

    @Override
    public void writeChar(char value) throws JMSException {
        checkWriteMode();
        try {
            getOutputStream().writeChar(value);
        } catch (IOException e) {
            throw new JMSException(e.getMessage());
        }

    }

    @Override
    public void writeInt(int value) throws JMSException {
        checkWriteMode();
        try {
            getOutputStream().writeInt(value);
        } catch (IOException e) {
            throw new JMSException(e.getMessage());
        }

    }

    @Override
    public void writeLong(long value) throws JMSException {
        checkWriteMode();
        try {
            getOutputStream().writeLong(value);
        } catch (IOException e) {
            throw new JMSException(e.getMessage());
        }
    }

    @Override
    public void writeFloat(float value) throws JMSException {
        checkWriteMode();
        try {
            getOutputStream().writeFloat(value);
        } catch (IOException e) {
            throw new JMSException(e.getMessage());
        }
    }

    @Override
    public void writeDouble(double value) throws JMSException {
        checkWriteMode();
        try {
            getOutputStream().writeDouble(value);
        } catch (IOException e) {
            throw new JMSException(e.getMessage());
        }
    }

    @Override
    public void writeUTF(String value) throws JMSException {
        checkWriteMode();
        try {
            getOutputStream().writeUTF(value);
        } catch (IOException e) {
            throw new JMSException(e.getMessage());
        }
    }

    @Override
    public void writeBytes(byte[] value) throws JMSException {
        checkWriteMode();
        try {
            getOutputStream().write(value);
        } catch (IOException e) {
            throw new JMSException(e.getMessage());
        }
    }

    @Override
    public void writeBytes(byte[] value, int offset, int length)
            throws JMSException {
        checkWriteMode();
        try {
            getOutputStream().write(value, offset, length);
        } catch (IOException e) {
            throw new JMSException(e.getMessage());
        }
    }

    @Override
    public void writeObject(Object value) throws JMSException {
        checkWriteMode();
        if (value instanceof Boolean) {
            writeBoolean(((Boolean) value).booleanValue());
        } else if (value instanceof Byte) {
            writeByte(((Byte) value).byteValue());
        } else if (value instanceof Short) {
            writeShort(((Short) value).shortValue());
        } else if (value instanceof Character) {
            writeChar(((Character) value).charValue());
        } else if (value instanceof Integer) {
            writeInt(((Integer) value).intValue());
        } else if (value instanceof Long) {
            writeLong(((Long) value).longValue());
        } else if (value instanceof Float) {
            writeFloat(((Float) value).floatValue());
        } else if (value instanceof Double) {
            writeDouble(((Double) value).doubleValue());
        } else if (value instanceof String) {
            writeUTF((String) value);
        } else if (value instanceof byte[]) {
            writeBytes((byte[]) value);
        } else if (value == null) {
            throw new NullPointerException(
                    "BytesMessage does not support null");
        } else {
            throw new MessageFormatException("Cannot write objects of type=" +
                    value.getClass().getName());
        }
    }

    @Override
    public void reset() throws JMSException {
        //in write-only mode
        if (!this.readOnlyBody) {
            try {
                //flush the outter stream, read the content from the underlying stream,
                //store it to the message and close the outter stream.
                output.flush();
                this.payload = byteOutput.toByteArray();
                output.close();
                //reset the input streams
                resetInputStreams();
            } catch (IOException e) {
                throw new JMSException(e.getMessage());
            }
        }
        //in read-only mode
        else {
            //reset the outter input stream to the beginning of the content
            try {
                this.input.reset();
            } catch (IOException e) {
                throw new JMSException(e.getMessage());
            }
        }
        //set read-only mode
        this.readOnlyBody = true;
    }

    /**
     * Closes the current input streams if possible and creates new ones
     * for reading the currently set message body.<p>
     */
    private void resetInputStreams() {
        try {
            this.input.close();
        } catch (Exception ignore) {
        }
        this.byteInput = new ByteArrayInputStream(this.payload);
        this.input = new DataInputStream(this.byteInput);
    }

    private DataInputStream getInputStream() throws JMSException {
        if (!readOnlyBody)
            throw new MessageNotReadableException("Message body is write-only");

        if (input == null) {
            byteInput = new ByteArrayInputStream(payload != null ? payload : new byte[0]);
            input = new DataInputStream(byteInput);
        }

        return input;
    }

    private DataOutputStream getOutputStream() throws JMSException {
        if (readOnlyBody)
            throw new MessageNotWriteableException("Message body is read-only");

        if (output == null) {
            byteOutput = new ByteArrayOutputStream(1024);
            output = new DataOutputStream(byteOutput);
        }
        payload = byteOutput.toByteArray();
        return output;
    }

    private void setToWriteOnlyMode() {
        try {
            input.close();
        } catch (Exception ignore) {
        }
        resetOutputStreams();
        this.readOnlyBody = false;
    }

    private void resetOutputStreams() {
        //flush and close output stream
        try {
            if (this.output != null) {
                this.output.flush();
                this.output.close();
            }
        } catch (Exception ignore) {
        }

        this.byteOutput = new ByteArrayOutputStream();
        this.output = new DataOutputStream(byteOutput);
    }

    @Override
    public String toString() {
        return "PulsarBytesMessage{" +
                "payload=" + Arrays.toString(payload) +
                ", input=" + input +
                ", output=" + output +
                ", byteInput=" + byteInput +
                ", byteOutput=" + byteOutput +
                '}';
    }
}
