package com.echostreams.pulsar.jms.utils;

import java.io.Closeable;
import java.util.Map;

public interface Serializer<T> extends Closeable {
    void configure(Map<String, ?> var1, boolean var2);

    byte[] serialize(T var2);

    void close();
}
