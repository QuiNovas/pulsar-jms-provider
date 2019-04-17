package com.echostreams.pulsar.jms.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class ArrayUtils {

    public static String[] convertToStringArray(Object value) {
        if (value == null) {
            return null;
        }

        String text = value.toString();
        if (text == null || text.isEmpty()) {
            return null;
        }

        StringTokenizer stok = new StringTokenizer(text, ",");
        final List<String> list = new ArrayList<String>();

        while (stok.hasMoreTokens()) {
            list.add(stok.nextToken());
        }

        String[] array = list.toArray(new String[list.size()]);
        return array;
    }

    public static String convertToString(String[] value) {
        if (value == null || value.length == 0) {
            return null;
        }

        StringBuffer result = new StringBuffer(String.valueOf(value[0]));
        for (int i = 1; i < value.length; i++) {
            result.append(",").append(value[i]);
        }

        return result.toString();
    }
}
