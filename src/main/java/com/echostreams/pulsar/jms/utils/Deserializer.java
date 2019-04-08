package com.echostreams.pulsar.jms.utils;

import java.io.Closeable;
import java.util.Map;

public interface Deserializer<T> extends Closeable {
    void configure(Map<String, ?> var1, boolean var2);

    T deserialize(byte[] var2);

    void close();
}
