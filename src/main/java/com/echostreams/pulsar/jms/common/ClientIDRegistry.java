package com.echostreams.pulsar.jms.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.InvalidClientIDException;
import java.util.HashSet;
import java.util.Set;

/**
 * <p>Global registry of JMS client IDs</p>
 */
public final class ClientIDRegistry {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientIDRegistry.class);

    private static ClientIDRegistry instance = null;

    /**
     * Get the singleton instance
     */
    public static synchronized ClientIDRegistry getInstance() {
        if (instance == null)
            instance = new ClientIDRegistry();
        return instance;
    }

    //-------------------------------------------------------------------------

    private Set<String> clientIDs = new HashSet<>();

    /**
     * Constructor (Private)
     */
    private ClientIDRegistry() {
        super();
    }

    /**
     * Register a new client ID
     */
    public synchronized void register(String clientID) throws InvalidClientIDException {
        if (!clientIDs.add(clientID)) {
            LOGGER.error("Client ID already exists : " + clientID);
            throw new InvalidClientIDException("Client ID already exists : " + clientID);
        }
        LOGGER.debug("Registered clientID : " + clientID);
    }

    /**
     * Register a new client ID
     */
    public synchronized void unregister(String clientID) {
        if (clientIDs.remove(clientID))
            LOGGER.debug("Unregistered clientID : " + clientID);
    }
}
