package com.echostreams.pulsar.jms.utils;

import java.io.UnsupportedEncodingException;
import java.util.Map;

public class StringSerializer implements Serializer<String> {
    private String encoding = "UTF8";

    public StringSerializer() {
    }

    public void configure(Map<String, ?> configs, boolean isKey) {
        String propertyName = isKey ? "key.serializer.encoding" : "value.serializer.encoding";
        Object encodingValue = configs.get(propertyName);
        if (encodingValue == null) {
            encodingValue = configs.get("serializer.encoding");
        }

        if (encodingValue != null && encodingValue instanceof String) {
            this.encoding = (String) encodingValue;
        }

    }

    @Override
    public byte[] serialize(String var2) {
        return new byte[0];
    }

    public byte[] serialize(String topic, String data) {
        try {
            return data == null ? null : data.getBytes(this.encoding);
        } catch (UnsupportedEncodingException var4) {
            throw new RuntimeException("Error when serializing string to byte[] due to unsupported encoding " + this.encoding);
        }
    }

    public void close() {
    }
}
