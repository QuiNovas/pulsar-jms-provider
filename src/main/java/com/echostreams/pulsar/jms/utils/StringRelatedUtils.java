package com.echostreams.pulsar.jms.utils;

import java.lang.reflect.Array;

public class StringRelatedUtils {

    public static boolean isEmpty(String text) {
        return text == null || text.length() == 0;
    }

    public static boolean isNotEmpty(String text) {
        return !isEmpty(text);
    }

    public static String implode(Object array, String delimiter) {
        StringBuilder sb = new StringBuilder();
        int len = Array.getLength(array);
        for (int i = 0; i < len; i++) {
            if (i > 0)
                sb.append(delimiter);
            Object value = Array.get(array, i);
            sb.append(String.valueOf(value));
        }
        return sb.toString();
    }
}
