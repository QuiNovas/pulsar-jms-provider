package com.echostreams.pulsar.jms.common;

import com.echostreams.pulsar.jms.message.*;

public class MessageGroup {

    public static final byte EMPTY = 1;
    public static final byte BYTES = 2;
    public static final byte MAP = 3;
    public static final byte OBJECT = 4;
    public static final byte STREAM = 5;
    public static final byte TEXT = 6;

    public static AbstractMessage createInstance(byte type) {
        switch (type) {
            case EMPTY:
                return new PulsarEmptyMessage();
            case BYTES:
                return new PulsarBytesMessage();
            case MAP:
                return new PulsarMapMessage();
            case OBJECT:
                return new PulsarObjectMessage();
            case STREAM:
                return new PulsarStreamMessage();
            case TEXT:
                return new PulsarTextMessage();

            default:
                throw new IllegalArgumentException("Unsupported message type : " + type);
        }
    }
}
