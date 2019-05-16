package com.echostreams.pulsar.jms.exceptions;

import javax.jms.*;

public class PulsarJMSException extends JMSException {

    private static final long serialVersionUID = -7153852455484731200L;

    public PulsarJMSException(String reason, String errorCode) {
        super(reason, errorCode);
    }

    public PulsarJMSException(String message, String errorCode, Throwable linkedException) {
        super(message, errorCode);
        if (linkedException != null) {
            if (linkedException instanceof Exception)
                setLinkedException((Exception) linkedException);
            else
                setLinkedException(new RuntimeException(linkedException));
        }
    }

    public static JMSRuntimeException createRuntimeException(Exception exception) {
        JMSRuntimeException result = null;
        JMSException source = null;

        if (!(exception instanceof JMSException)) {
            throw new JMSRuntimeException(exception.getMessage(), null, exception);
        } else {
            source = (JMSException) exception;
        }

        if (source instanceof javax.jms.IllegalStateException) {
            result = new IllegalStateRuntimeException(source.getMessage(), source.getErrorCode(), source);
        } else if (source instanceof InvalidClientIDException) {
            result = new InvalidClientIDRuntimeException(source.getMessage(), source.getErrorCode(), source);
        } else if (source instanceof InvalidDestinationException) {
            result = new InvalidDestinationRuntimeException(source.getMessage(), source.getErrorCode(), source);
        } else if (source instanceof InvalidSelectorException) {
            result = new InvalidSelectorRuntimeException(source.getMessage(), source.getErrorCode(), source);
        } else if (source instanceof JMSSecurityException) {
            result = new JMSSecurityRuntimeException(source.getMessage(), source.getErrorCode(), source);
        } else if (source instanceof MessageFormatException) {
            result = new MessageFormatRuntimeException(source.getMessage(), source.getErrorCode(), source);
        } else if (source instanceof MessageNotWriteableException) {
            result = new MessageNotWriteableRuntimeException(source.getMessage(), source.getErrorCode(), source);
        } else if (source instanceof ResourceAllocationException) {
            result = new ResourceAllocationRuntimeException(source.getMessage(), source.getErrorCode(), source);
        } else if (source instanceof TransactionInProgressException) {
            result = new TransactionInProgressRuntimeException(source.getMessage(), source.getErrorCode(), source);
        } else if (source instanceof TransactionRolledBackException) {
            result = new TransactionRolledBackRuntimeException(source.getMessage(), source.getErrorCode(), source);
        } else {
            result = new JMSRuntimeException(source.getMessage(), source.getErrorCode(), source);
        }

        return result;
    }
}
