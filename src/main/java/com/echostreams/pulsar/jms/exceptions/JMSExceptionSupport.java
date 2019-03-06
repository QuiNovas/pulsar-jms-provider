package com.echostreams.pulsar.jms.exceptions;

import javax.jms.IllegalStateException;
import javax.jms.*;

/**
 * Exception support class.
 */
public final class JMSExceptionSupport {

    private JMSExceptionSupport() {}

    public static JMSException create(String message, Throwable cause) {
        if (cause instanceof JMSException) {
            return (JMSException) cause;
        }

        if (cause.getCause() instanceof JMSException) {
            return (JMSException) cause.getCause();
        }

        if (message == null || message.isEmpty()) {
            message = cause.getMessage();
            if (message == null || message.isEmpty()) {
                message = cause.toString();
            }
        }

        JMSException exception = new JMSException(message);
        if (cause instanceof Exception) {
            exception.setLinkedException((Exception) cause);
        }
        exception.initCause(cause);
        return exception;
    }

    public static JMSException create(Throwable cause) {
        return create(null, cause);
    }

    public static MessageEOFException createMessageEOFException(Throwable cause) {
        String message = cause.getMessage();
        if (message == null || message.length() == 0) {
            message = cause.toString();
        }

        MessageEOFException exception = new MessageEOFException(message);
        if (cause instanceof Exception) {
            exception.setLinkedException((Exception) cause);
        }
        exception.initCause(cause);
        return exception;
    }

    public static MessageFormatException createMessageFormatException(Throwable cause) {
        String message = cause.getMessage();
        if (message == null || message.length() == 0) {
            message = cause.toString();
        }

        MessageFormatException exception = new MessageFormatException(message);
        if (cause instanceof Exception) {
            exception.setLinkedException((Exception) cause);
        }
        exception.initCause(cause);
        return exception;
    }

    public static JMSRuntimeException createRuntimeException(Exception exception) {
        JMSRuntimeException result = null;
        JMSException source = null;

        if (!(exception instanceof JMSException)) {
            throw new JMSRuntimeException(exception.getMessage(), null, exception);
        } else {
            source = (JMSException) exception;
        }

        if (source instanceof IllegalStateException) {
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
