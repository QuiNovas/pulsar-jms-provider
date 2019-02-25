package com.echostreams.pulsar.jms.utils;

import com.echostreams.pulsar.jms.message.PulsarMessage;
import com.echostreams.pulsar.jms.message.PulsarTextMessage;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.util.Enumeration;

/**
 * Utility functions to copy and normalize JMS messages.</p>
 */
public final class MessageUtils {

    public static PulsarMessage transformMessage( Message sourceMsg ) throws JMSException {
        if (sourceMsg instanceof PulsarMessage) {
            return (PulsarMessage) sourceMsg;

        }else{
            PulsarMessage pulsarMessage = null;
            if (sourceMsg instanceof TextMessage) {
                TextMessage textMsg = (TextMessage) sourceMsg;
                PulsarTextMessage msg = new PulsarTextMessage();
                msg.setText(textMsg.getText());
                pulsarMessage = msg;
            }
            copyProperties(sourceMsg, pulsarMessage);
            return pulsarMessage;
        }
    }

    public static void copyProperties(Message fromMessage, Message toMessage) throws JMSException {
        toMessage.setJMSMessageID(fromMessage.getJMSMessageID());
        toMessage.setJMSCorrelationID(fromMessage.getJMSCorrelationID());
        //TODO nee to check dest type of queue/topic/tempqueue/temptopic
        /*toMessage.setJMSReplyTo(transformDestination(fromMessage.getJMSReplyTo()));
        toMessage.setJMSDestination(transformDestination(fromMessage.getJMSDestination()));*/
        toMessage.setJMSDeliveryMode(fromMessage.getJMSDeliveryMode());
        toMessage.setJMSRedelivered(fromMessage.getJMSRedelivered());
        toMessage.setJMSType(fromMessage.getJMSType());
        toMessage.setJMSExpiration(fromMessage.getJMSExpiration());
        toMessage.setJMSPriority(fromMessage.getJMSPriority());
        toMessage.setJMSTimestamp(fromMessage.getJMSTimestamp());

        Enumeration propertyNames = fromMessage.getPropertyNames();

        while (propertyNames.hasMoreElements()) {
            String name = propertyNames.nextElement().toString();
            Object obj = fromMessage.getObjectProperty(name);
            toMessage.setObjectProperty(name, obj);
        }
    }
}
