package com.echostreams.pulsar.jms.utils;

public final class ArrayRelatedUtils {

    public static byte[] copy(byte[] array) {
        byte[] result = new byte[array.length];
        System.arraycopy(array, 0, result, 0, array.length);
        return result;
    }

    public static byte[] extend(byte[] array, int newSize) {
        byte[] result = new byte[newSize];
        System.arraycopy(array, 0, result, 0, Math.min(array.length, newSize));
        return result;
    }

    public static int[] extend(int[] array, int newSize) {
        int[] result = new int[newSize];
        System.arraycopy(array, 0, result, 0, Math.min(array.length, newSize));
        return result;
    }
}
