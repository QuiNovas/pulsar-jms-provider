package com.echostreams.pulsar.jms.utils;

public class StringRelatedUtils {

    public static boolean isEmpty(String text) {
        return text == null || text.length() == 0;
    }

    public static boolean isNotEmpty(String text) {
        return !isEmpty(text);
    }
}
