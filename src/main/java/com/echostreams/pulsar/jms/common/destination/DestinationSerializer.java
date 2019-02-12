package com.echostreams.pulsar.jms.common.destination;

import com.echostreams.pulsar.jms.utils.RawDataBuffer;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Topic;

public final class DestinationSerializer {
    private static final byte NO_DESTINATION = 0;
    private static final byte TYPE_QUEUE = 1;
    private static final byte TYPE_TOPIC = 2;

    /**
     * Serialize a destination to the given stream
     */
    public static void serializeTo(Destination destination, RawDataBuffer out) {
        try {
            if (destination == null) {
                out.writeByte(NO_DESTINATION);
            } else if (destination instanceof Queue) {
                out.writeByte(TYPE_QUEUE);
                out.writeUTF(((Queue) destination).getQueueName());
            } else if (destination instanceof Topic) {
                out.writeByte(TYPE_TOPIC);
                out.writeUTF(((Topic) destination).getTopicName());
            } else
                throw new IllegalArgumentException("Unsupported destination : " + destination);
        } catch (JMSException e) {
            throw new IllegalArgumentException("Cannot serialize destination : " + e.getMessage());
        }
    }

    /**
     * Unserialize a destination from the given stream
     */
    public static DestinationRef unserializeFrom(RawDataBuffer in) {
        int type = in.readByte();
        if (type == NO_DESTINATION)
            return null;

        String destinationName = in.readUTF();
        switch (type) {
            case TYPE_QUEUE:
                return new QueueRef(destinationName);
            case TYPE_TOPIC:
                return new TopicRef(destinationName);
            default:
                throw new IllegalArgumentException("Unsupported destination type : " + type);
        }
    }
}
