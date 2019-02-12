package com.echostreams.pulsar.jms.common;

import com.echostreams.pulsar.jms.utils.JavaRelatedUtils;
import com.echostreams.pulsar.jms.utils.id.IntegerID;

import javax.jms.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class AbstractMessageHandler {

    // Unique ID
    protected IntegerID id;

    // Parents
    protected AbstractConnection connection;
    protected AbstractSession session;

    // Destination
    protected Destination destination;

    // runtime
    protected boolean closed;
    protected ReadWriteLock externalAccessLock = new ReentrantReadWriteLock();

    /**
     * Constructor
     */
    public AbstractMessageHandler(AbstractSession session,
                                  Destination destination,
                                  IntegerID handlerId) {
        this.session = session;
        this.connection = session.getConnection();
        this.destination = destination;
        this.id = handlerId;
    }

    public final IntegerID getId() {
        return id;
    }

    public final AbstractSession getSession() {
        return session;
    }

    protected final void checkNotClosed() throws JMSException
    {
        if (closed)
            throw new javax.jms.IllegalStateException("Message handler is closed"); // [JMS SPEC]
    }

    public final void getEntitiesDescription( StringBuilder sb )
    {
        sb.append(toString());
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append(JavaRelatedUtils.getShortClassName(getClass()));
        sb.append("[#");
        sb.append(id);
        sb.append("]");
        if (destination != null)
        {
            sb.append("(destination=");
            sb.append(destination);
            sb.append(")");
        }

        return sb.toString();
    }

}
