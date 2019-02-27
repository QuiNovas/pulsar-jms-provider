package com.echostreams.pulsar.jms.utils;

import java.io.UnsupportedEncodingException;
import java.util.Map;

public class StringDeserializer implements Deserializer<String> {
    private String encoding = "UTF8";

    public StringDeserializer() {
    }

    public void configure(Map<String, ?> configs, boolean isKey) {
        String propertyName = isKey ? "key.deserializer.encoding" : "value.deserializer.encoding";
        Object encodingValue = configs.get(propertyName);
        if (encodingValue == null) {
            encodingValue = configs.get("deserializer.encoding");
        }

        if (encodingValue != null && encodingValue instanceof String) {
            this.encoding = (String) encodingValue;
        }

    }

    @Override
    public String deserialize(byte[] var2) {
        return null;
    }

    public String deserialize(String topic, byte[] data) {
        try {
            return data == null ? null : new String(data, this.encoding);
        } catch (UnsupportedEncodingException var4) {
            throw new RuntimeException("Error when deserializing byte[] to string due to unsupported encoding " + this.encoding);
        }
    }

    public void close() {
    }
}
