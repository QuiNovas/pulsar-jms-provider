package com.echostreams.pulsar.jms.utils;

import java.io.*;

public final class SerializationRelatedUtils {
    /**
     * Object to byte array
     */
    public static byte[] toByteArray(Serializable object) {
        try {
            ByteArrayOutputStream buf = new ByteArrayOutputStream(1024);
            ObjectOutputStream objOut = new ObjectOutputStream(buf);
            objOut.writeObject(object);
            objOut.close();
            return buf.toByteArray();
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot serialize object : " + e.toString());
        }
    }

    /**
     * Byte array to object
     */
    public static Serializable fromByteArray(byte[] data) {
        try {
            ByteArrayInputStream buf = new ByteArrayInputStream(data);
            ObjectInputStream objIn = new ObjectInputStream(buf);
            Serializable response = (Serializable) objIn.readObject();
            objIn.close();
            return response;
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot deserialize object : " + e.toString());
        }
    }

    public static void writeInt(int v, OutputStream out) throws IOException {
        out.write((v >>> 24) & 0xFF);
        out.write((v >>> 16) & 0xFF);
        out.write((v >>> 8) & 0xFF);
        out.write((v >>> 0) & 0xFF);
    }
}
