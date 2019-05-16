package com.echostreams.pulsar.jms.exceptions;

import org.junit.Test;

import javax.jms.*;
import java.io.IOException;

public class PulsarJMSExceptionTest {

    @Test(expected = JMSRuntimeException.class)
    public void testConvertsJMSExceptionToJMSRuntimeException() {
        throw PulsarJMSException.createRuntimeException(new JMSException("error"));
    }

    @Test(expected = IllegalStateRuntimeException.class)
    public void testConvertsIllegalStateExceptionToIlleglStateRuntimeException() {
        throw PulsarJMSException.createRuntimeException(new javax.jms.IllegalStateException("error"));
    }

    @Test(expected = InvalidClientIDRuntimeException.class)
    public void testConvertsInvalidClientIDExceptionToInvalidClientIDRuntimeException() {
        throw PulsarJMSException.createRuntimeException(new InvalidClientIDException("error"));
    }

    @Test(expected = InvalidDestinationRuntimeException.class)
    public void testConvertsInvalidDestinationExceptionToInvalidDestinationRuntimeException() {
        throw PulsarJMSException.createRuntimeException(new InvalidDestinationException("error"));
    }

    @Test(expected = InvalidSelectorRuntimeException.class)
    public void testConvertsInvalidSelectorExceptionToInvalidSelectorRuntimeException() {
        throw PulsarJMSException.createRuntimeException(new InvalidSelectorException("error"));
    }

    @Test(expected = JMSSecurityRuntimeException.class)
    public void testConvertsJMSSecurityExceptionToJMSSecurityRuntimeException() {
        throw PulsarJMSException.createRuntimeException(new JMSSecurityException("error"));
    }

    @Test(expected = MessageFormatRuntimeException.class)
    public void testConvertsMessageFormatExceptionToMessageFormatRuntimeException() {
        throw PulsarJMSException.createRuntimeException(new MessageFormatException("error"));
    }

    @Test(expected = MessageNotWriteableRuntimeException.class)
    public void testConvertsMessageNotWriteableExceptionToMessageNotWriteableRuntimeException() {
        throw PulsarJMSException.createRuntimeException(new MessageNotWriteableException("error"));
    }

    @Test(expected = ResourceAllocationRuntimeException.class)
    public void testConvertsResourceAllocationExceptionToResourceAllocationRuntimeException() {
        throw PulsarJMSException.createRuntimeException(new ResourceAllocationException("error"));
    }

    @Test(expected = TransactionInProgressRuntimeException.class)
    public void testConvertsTransactionInProgressExceptionToTransactionInProgressRuntimeException() {
        throw PulsarJMSException.createRuntimeException(new TransactionInProgressException("error"));
    }

    @Test(expected = TransactionRolledBackRuntimeException.class)
    public void testConvertsTransactionRolledBackExceptionToTransactionRolledBackRuntimeException() {
        throw PulsarJMSException.createRuntimeException(new TransactionRolledBackException("error"));
    }

    @Test(expected = JMSRuntimeException.class)
    public void testConvertsNonJMSExceptionToJMSRuntimeException() {
        throw PulsarJMSException.createRuntimeException(new IOException());
    }
}
