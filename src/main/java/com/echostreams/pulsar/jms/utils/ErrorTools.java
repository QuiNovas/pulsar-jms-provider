package com.echostreams.pulsar.jms.utils;

import org.slf4j.Logger;

import javax.jms.JMSException;

public final class ErrorTools {
    public static void log(JMSException e, Logger log) {
        log(null, e, log);
    }

    public static void log(String context, JMSException e, Logger log) {
        StringBuilder message = new StringBuilder();
        if (StringRelatedUtils.isNotEmpty(context)) {
            message.append("[");
            message.append(context);
            message.append("] ");
        }
        if (StringRelatedUtils.isNotEmpty(e.getErrorCode())) {
            message.append("error={");
            message.append(e.getErrorCode());
            message.append("} ");
        }
        message.append(StringRelatedUtils.isNotEmpty(e.getMessage()));
        log.error(message.toString());
        if (e.getLinkedException() != null)
            log.error("Linked exception was :", e.getLinkedException());
    }
}
