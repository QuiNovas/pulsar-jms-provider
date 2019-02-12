package com.echostreams.pulsar.jms.common;

import com.echostreams.pulsar.jms.utils.id.IntegerID;

import javax.jms.Destination;
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
}
