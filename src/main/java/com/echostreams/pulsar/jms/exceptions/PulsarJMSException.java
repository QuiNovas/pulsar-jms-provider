package com.echostreams.pulsar.jms.exceptions;

import javax.jms.JMSException;

public class PulsarJMSException extends JMSException {

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
}
