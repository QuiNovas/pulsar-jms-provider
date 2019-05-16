package com.echostreams.pulsar.jms.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ArrayUtilsTest {

    @Test
    public void testConvertToStringArray() throws Exception {
        assertNull(ArrayUtils.convertToStringArray(null));
        assertNull(ArrayUtils.convertToStringArray(""));

        String[] array = ArrayUtils.convertToStringArray("test");
        assertEquals(1, array.length);
        assertEquals("test", array[0]);

        array = ArrayUtils.convertToStringArray("test1,test2");
        assertEquals(2, array.length);
        assertEquals("test1", array[0]);
        assertEquals("test2", array[1]);

        array = ArrayUtils.convertToStringArray("test1,test2,test3");
        assertEquals(3, array.length);
        assertEquals("test1", array[0]);
        assertEquals("test2", array[1]);
        assertEquals("test3", array[2]);
    }

    @Test
    public void testConvertToString() throws Exception {
        assertNull(ArrayUtils.convertToString(null));
        assertNull(ArrayUtils.convertToString(new String[]{}));

        assertEquals("", ArrayUtils.convertToString(new String[]{""}));
        assertEquals("test", ArrayUtils.convertToString(new String[]{"test"}));
        assertEquals("test1,test2", ArrayUtils.convertToString(new String[]{"test1", "test2"}));
        assertEquals("test1,test2,test3", ArrayUtils.convertToString(new String[]{"test1", "test2", "test3"}));
    }
}
