package com.echostreams.pulsar.jms.message;

import com.echostreams.pulsar.jms.PulsarJMSProvider;
import com.echostreams.pulsar.jms.common.AbstractMessage;
import com.echostreams.pulsar.jms.common.AbstractMessageConsumer;
import com.echostreams.pulsar.jms.config.PulsarConfiguration;
import com.echostreams.pulsar.jms.config.PulsarSession;
import com.echostreams.pulsar.jms.queue.PulsarQueue;
import com.echostreams.pulsar.jms.topic.PulsarTopic;
import com.echostreams.pulsar.jms.utils.ErrorTools;
import com.echostreams.pulsar.jms.utils.id.IntegerID;
import com.echostreams.pulsar.jms.utils.id.UUIDProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Topic;

public class PulsarMessageConsumer extends AbstractMessageConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarMessageConsumer.class);

    // Parent engine
    protected PulsarJMSProvider pulsarJMSProvider;

    // Parsed selector
    protected String messageSelector;

    // Runtime
    private String subscriberId;
    private PulsarQueue pulsarQueue;
    private PulsarTopic pulsarTopic;
    private boolean traceEnabled;
    private boolean receiving;
    private TransactionSet transactionSet;

    // Specific to receive mode
    private Object receiveLock = new Object();

    // Used for interaction with the remote layer
    private NotificationProxy notificationProxy;
    private int prefetchSize;
    private int prefetchCapacity;
    private Object prefetchLock = new Object();

    // Settings
    private boolean logListenersFailures;

    public PulsarMessageConsumer(PulsarJMSProvider pulsarJMSProvider,
                                PulsarSession session,
                                Destination destination,
                                String messageSelector,
                                boolean noLocal,
                                IntegerID consumerId,
                                String subscriberId) throws JMSException
    {
        super(session,destination,messageSelector,noLocal,consumerId);
        this.pulsarJMSProvider = pulsarJMSProvider;
        this.session = session;
        this.messageSelector = messageSelector;
        this.traceEnabled = log.isTraceEnabled();
        this.transactionSet = session.getTransactionSet();
        this.notificationProxy = session.getNotificationProxy();
        this.prefetchCapacity = this.prefetchSize = engine.getSetup().getConsumerPrefetchSize();
        this.logListenersFailures = getSettings().getBooleanProperty(PulsarConfiguration.DELIVERY_LOG_LISTENERS_FAILURES, false);
        this.subscriberId = subscriberId != null ? subscriberId : UUIDProvider.getInstance().getShortUUID();
    }



    @Override
    protected AbstractMessage receiveFromDestination(long timeout, boolean duplicateRequired) throws JMSException {
        return null;
    }

    @Override
    protected boolean shouldLogListenersFailures() {
        return logListenersFailures;
    }

    @Override
    protected void wakeUp() throws JMSException {

    }

    public final boolean getNoLocal()
    {
        return noLocal;
    }

    private void unregister()
    {
        if (pulsarTopic != null)
            pulsarTopic.unregisterConsumer(this);
        if (pulsarQueue != null)
        {
            pulsarQueue.unregisterConsumer(this);

            try
            {
                // Drop volatile topic subscriber queue
                if ((destination instanceof Topic) && !isDurable())
                {
                    pulsarQueue.close();
                    ((PulsarSession)session).deleteQueue(pulsarQueue.getName());
                }
            }
            catch (JMSException e)
            {
                ErrorTools.log(e, LOGGER);
            }
        }
    }

    public boolean isDurable()
    {
        return false;
    }
}
