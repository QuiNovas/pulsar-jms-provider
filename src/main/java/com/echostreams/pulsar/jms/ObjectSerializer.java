package com.echostreams.pulsar.jms;

import com.echostreams.pulsar.jms.utils.Deserializer;
import com.echostreams.pulsar.jms.utils.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;

public class ObjectSerializer<T> implements Serializer<T>, Deserializer<T> {
    private Logger logger = LoggerFactory.getLogger(getClass());

    /* (non-Javadoc)
     * @see org.apache.kafka.common.serialization.Serializer#serialize(java.lang.String, java.lang.Object)
     */
    public byte[] serialize(String paramString, T paramT) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(baos)) {
            out.writeObject(paramT);
        } catch (IOException e) {
            logger.error("Failed to serialize object.", e);
            throw new RuntimeException("Failed to serialize object.", e);
        }
        return baos.toByteArray();
    }

    /* (non-Javadoc)
     * @see org.apache.kafka.common.serialization.Serializer#close()
     */
    public void close() {
        // NOOP
    }

    /* (non-Javadoc)
     * @see org.apache.kafka.common.serialization.Serializer#configure(java.util.Map, boolean)
     */
    public void configure(Map<String, ?> paramMap, boolean paramBoolean) {
        // NOOP
    }

    /* (non-Javadoc)
     * @see org.apache.kafka.common.serialization.Deserializer#deserialize(java.lang.String, byte[])
     */
    @Override
    public T deserialize(String arg0, byte[] arg1) {
        ByteArrayInputStream bais = new ByteArrayInputStream(arg1);
        try (ObjectInputStream in = new ObjectInputStream(bais)) {
            return (T) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            logger.error("Failed to serialize object.", e);
            throw new RuntimeException("Failed to deserialize object.", e);
        }
    }

}
