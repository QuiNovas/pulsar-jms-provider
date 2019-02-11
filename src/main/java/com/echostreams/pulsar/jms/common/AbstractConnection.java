package com.echostreams.pulsar.jms.common;

import com.echostreams.pulsar.jms.utils.PulsarJMSException;
import com.echostreams.pulsar.jms.utils.StringRelatedUtils;
import com.echostreams.pulsar.jms.utils.id.IntegerID;
import com.echostreams.pulsar.jms.utils.id.IntegerIDProvider;
import com.echostreams.pulsar.jms.utils.id.UUIDProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import javax.jms.IllegalStateException;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class AbstractConnection implements Connection {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractConnection.class);

    // ID and client ID handling
    protected String id = UUIDProvider.getInstance().getShortUUID();
    protected String clientID;

    // Runtime
    protected boolean started;
    protected boolean closed;
    private ExceptionListener exceptionListener;
    private Set<String> temporaryQueues = new HashSet<>();
    private Set<String> temporaryTopics = new HashSet<>();
    protected IntegerIDProvider idProvider = new IntegerIDProvider();
    protected ReadWriteLock externalAccessLock = new ReentrantReadWriteLock();
    private Object exceptionListenerLock = new Object();

    // Children
    private Map<IntegerID,AbstractSession> sessions = new Hashtable<>();

    public AbstractConnection( String clientID )
    {
        this.clientID = clientID;
    }

    public String getId()
    {
        return id;
    }

    @Override
    public String getClientID() throws JMSException {
        externalAccessLock.readLock().lock();
        try
        {
            if (clientID == null)
                throw new InvalidClientIDException("Client ID not set");
            return clientID;
        }
        finally
        {
            externalAccessLock.readLock().unlock();
        }
    }

    @Override
    public void setClientID(String s) throws JMSException {
        externalAccessLock.readLock().lock();
        try
        {
            isClosed();
            if (StringRelatedUtils.isEmpty(clientID))
                throw new InvalidClientIDException("Empty client ID");
            if (this.clientID != null)
                throw new IllegalStateException("Client ID is already set"); // [JMS SPEC]
        }
        finally
        {
            externalAccessLock.readLock().unlock();
        }
    }

    protected final void isClosed() throws JMSException
    {
        if (closed)
            throw new PulsarJMSException("Connection is closed","CONNECTION_CLOSED");
    }

    @Override
    public ExceptionListener getExceptionListener() throws JMSException {
        synchronized (exceptionListenerLock)
        {
            return exceptionListener;
        }
    }

    @Override
    public void setExceptionListener(ExceptionListener exceptionListener) throws JMSException {
        synchronized (exceptionListenerLock)
        {
            isClosed();
            this.exceptionListener = exceptionListener;
        }
    }

    @Override
    public void close() throws JMSException {
        externalAccessLock.writeLock().lock();
        try
        {
            if (closed)
                return;
            closed = true;
            onConnectionClose();
        }
        finally
        {
            externalAccessLock.writeLock().unlock();
        }
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Destination destination, String s, ServerSessionPool serverSessionPool, int i) throws JMSException {
        throw new PulsarJMSException("Unsupported feature","UNSUPPORTED_FEATURE");
    }

    @Override
    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String s, String s1, ServerSessionPool serverSessionPool, int i) throws JMSException {
        throw new PulsarJMSException("Unsupported feature","UNSUPPORTED_FEATURE");
    }

    protected void onConnectionClose()
    {
        // Close remaining sessions
        closeRemainingSessions();

        // Drop temporary queues
        dropTemporaryQueues();
    }

    private void closeRemainingSessions()
    {
        if (sessions == null)
            return;

        List<AbstractSession> sessionsToClose = new ArrayList<>(sessions.size());
        synchronized (sessions)
        {
            sessionsToClose.addAll(sessions.values());
        }
        for (int n = 0 ; n < sessionsToClose.size() ; n++)
        {
            Session session = sessionsToClose.get(n);
            LOGGER.debug("Auto-closing unclosed session : "+session);
            try
            {
                session.close();
            }
            catch (JMSException e)
            {
                LOGGER.error("",e);
            }
        }
    }

    public final void exceptionOccured( JMSException exception )
    {
        try
        {
            synchronized (exceptionListenerLock)
            {
                if (exceptionListener != null)
                    exceptionListener.onException(exception);
            }
        }
        catch (Exception e)
        {
            LOGGER.error("Exception listener failed",e);
        }
    }

    public final void registerTemporaryQueue( String queueName )
    {
        synchronized (temporaryQueues)
        {
            temporaryQueues.add(queueName);
        }
    }

    public final void unregisterTemporaryQueue( String queueName )
    {
        synchronized (temporaryQueues)
        {
            temporaryQueues.remove(queueName);
        }
    }

    public final boolean isRegisteredTemporaryQueue( String queueName )
    {
        synchronized (temporaryQueues)
        {
            return temporaryQueues.contains(queueName);
        }
    }

    public final void registerTemporaryTopic( String topicName )
    {
        synchronized (temporaryTopics)
        {
            temporaryTopics.add(topicName);
        }
    }

    public final void unregisterTemporaryTopic( String topicName )
    {
        synchronized (temporaryTopics)
        {
            temporaryTopics.remove(topicName);
        }
    }

    public final boolean isRegisteredTemporaryTopic( String topicName )
    {
        synchronized (temporaryTopics)
        {
            return temporaryTopics.contains(topicName);
        }
    }

    private void dropTemporaryQueues()
    {
        synchronized (temporaryQueues)
        {
            Iterator<String> remainingQueues = temporaryQueues.iterator();
            while (remainingQueues.hasNext())
            {
                String queueName = remainingQueues.next();
                try
                {
                    deleteTemporaryQueue(queueName);
                }
                catch (JMSException e)
                {
                    LOGGER.error("",e);
                }
            }
        }
    }

    public abstract void deleteTemporaryQueue( String queueName ) throws JMSException;

    public abstract void deleteTemporaryTopic( String topicName ) throws JMSException;

    protected final void wakeUpLocalConsumers()
    {
        List<AbstractSession> sessionsSnapshot = new ArrayList<>(sessions.size());
        synchronized (sessions)
        {
            sessionsSnapshot.addAll(sessions.values());
        }
        for(int n=0;n<sessionsSnapshot.size();n++)
        {
            AbstractSession session = sessionsSnapshot.get(n);
            //session.wakeUpConsumers();
        }
    }

    /**
     * Wait for sessions to finish the current deliveridispatching
     */
    protected final void waitForDeliverySync()
    {
        List<AbstractSession> sessionsSnapshot = new ArrayList<>(sessions.size());
        synchronized (sessions)
        {
            sessionsSnapshot.addAll(sessions.values());
        }
        for(int n=0;n<sessionsSnapshot.size();n++)
        {
            AbstractSession session = sessionsSnapshot.get(n);
            //session.waitForDeliverySync();
        }
    }

    /**
     * Lookup a registered session
     */
    public final AbstractSession lookupRegisteredSession( IntegerID sessionId )
    {
        return sessions.get(sessionId);
    }

    /**
     * Register a session
     */
    protected final void registerSession( AbstractSession sessionToAdd )
    {
        //if (sessions.put(sessionToAdd.getId(),sessionToAdd) != null)
          //  throw new IllegalArgumentException("Session "+sessionToAdd.getId()+" already exists");
    }

    /**
     * Unregister a session
     */
    public final void unregisterSession( AbstractSession sessionToRemove )
    {
        //if (sessions.remove(sessionToRemove.getId()) == null)
          //  LOGGER.warn("Unknown session : "+sessionToRemove);
    }

    @Override
    protected void finalize() throws Throwable
    {
        if (externalAccessLock != null && !closed)
        {
            LOGGER.warn("Connection was not properly closed, closing it now.");
            try
            {
                close();
            }
            catch (Throwable e)
            {
                LOGGER.error("Could not auto-close connection",e);
            }
        }
    }

    /**
     * Check if the connection is started
     * NOT SYNCHRONIZED TO AVOID DEADLOCKS
     */
    public boolean isStarted()
    {
        return started && !closed;
    }

    /**
     * Get the number of active sessions for this connection
     * @return the number of active sessions for this connection
     */
    public int getSessionsCount()
    {
        return sessions.size();
    }

    /**
     * Get the number of active producers for this connection
     * @return the number of active producers for this connection
     */
    public int getConsumersCount()
    {
        synchronized (sessions)
        {
            if (sessions.isEmpty())
                return 0;

            int total = 0;
            Iterator<AbstractSession>sessionsIterator = sessions.values().iterator();
            while (sessionsIterator.hasNext())
            {
                AbstractSession session = sessionsIterator.next();
                //total += session.getConsumersCount();
            }
            return total;
        }
    }

    /**
     * Get the number of active producers for this connection
     * @return the number of active producers for this connection
     */
    public int getProducersCount()
    {
        synchronized (sessions)
        {
            if (sessions.isEmpty())
                return 0;

            int total = 0;
            Iterator<AbstractSession> sessionsIterator = sessions.values().iterator();
            while (sessionsIterator.hasNext())
            {
                AbstractSession session = sessionsIterator.next();
                //total += session.getProducersCount();
            }
            return total;
        }
    }

    /**
     * Get a description of entities held by this object
     */
    public void getEntitiesDescription( StringBuilder sb )
    {
        sb.append(toString());
        sb.append("{");
        synchronized (sessions)
        {
            if (!sessions.isEmpty())
            {
                int pos = 0;
                Iterator<AbstractSession> sessionsIterator = sessions.values().iterator();
                while (sessionsIterator.hasNext())
                {
                    AbstractSession session = sessionsIterator.next();
                    if (pos++ > 0)
                        sb.append(",");
                    //session.getEntitiesDescription(sb);
                }
            }
        }
        sb.append("}");
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append("Connection[#");
        sb.append(id);
        sb.append("](started=");
        sb.append(started);
        sb.append(")");

        return sb.toString();
    }




}
