package com.echostreams.pulsar.jms.utils;

public class JavaRelatedUtils {

    public static String getShortClassName( Class<?> clazz )
    {
        String className = clazz.getName();
        int idx = className.lastIndexOf('.');
        return (idx == -1 ? className : className.substring(idx+1));
    }

    public static String getCallerMethodName( int offset )
    {
        StackTraceElement[] stack = new Exception().getStackTrace();
        return stack[offset+1].getMethodName();
    }
}
