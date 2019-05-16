package com.echostreams.pulsar.jms.utils;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;

public class CommonUtilsTest {

    @Test
    public void testCopy() {
        byte[] dummy = {(byte) 1, (byte) 2, (byte) 3};

        assertNotNull(CommonUtils.copy(dummy));
        assertArrayEquals(dummy, CommonUtils.copy(dummy));
    }
}
