package com.echostreams.pulsar.jms.utils;

public class SystemTools {

    public static String replaceSystemProperties(String value) {
        // Dumb case
        if (value == null || value.length() == 0)
            return value;

        StringBuilder sb = new StringBuilder();

        int pos = 0;
        int start;

        while ((start = value.indexOf("${", pos)) != -1) {
            if (start > pos)
                sb.append(value.substring(pos, start));

            int end = value.indexOf('}', start + 2);
            if (end == -1) {
                pos = start;
                break;
            }

            String varName = value.substring(start + 2, end);
            String varValue = System.getProperty(varName, "${" + varName + "}");
            sb.append(varValue);

            pos = end + 1;
        }

        // Append remaining
        if (pos < value.length())
            sb.append(value.substring(pos));

        return sb.toString();
    }
}
