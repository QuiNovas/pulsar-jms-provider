package com.echostreams.pulsar.jms.jndi;

import com.echostreams.pulsar.jms.client.PulsarConnectionFactory;
import com.echostreams.pulsar.jms.client.PulsarQueue;
import com.echostreams.pulsar.jms.client.PulsarTopic;

import javax.jms.Queue;
import javax.jms.Topic;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class PulsarInitialContextFactory implements InitialContextFactory {

    static final String[] DEFAULT_CONNECTION_FACTORY_NAMES = {
            "ConnectionFactory", "QueueConnectionFactory", "TopicConnectionFactory" };

    static final String CONNECTION_FACTORY_KEY_PREFIX = "connectionfactory.";
    static final String QUEUE_KEY_PREFIX = "queue.";
    static final String TOPIC_KEY_PREFIX = "topic.";
    static final String CONNECTION_FACTORY_DEFAULT_KEY_PREFIX = "default." + CONNECTION_FACTORY_KEY_PREFIX;
    static final String CONNECTION_FACTORY_PROPERTY_KEY_PREFIX = "property." + CONNECTION_FACTORY_KEY_PREFIX;

    public PulsarInitialContextFactory() {
        super();
    }

    /**
     * Creates an Initial Context for beginning name resolution.
     * Special requirements of this context are supplied
     * using <code>environment</code>.
     * <p>
     * The environment parameter is owned by the caller.
     * The implementation will not modify the object or keep a reference
     * to it, although it may keep a reference to a clone or copy.
     *
     * @param environment The possibly null environment
     *                    specifying information to be used in the creation
     *                    of the initial context.
     * @return A non-null initial context object that implements the Context
     * interface.
     * @throws NamingException If cannot create an initial context.
     */
    @Override
    public Context getInitialContext(Hashtable<?, ?> environment) throws NamingException {

        //Copy environment
        Hashtable<Object, Object> environmentCopy = new Hashtable<Object, Object>();
        environmentCopy.putAll(environment);

        // Create the bindings for the context using environment
        Map<String, Object> bindings = new ConcurrentHashMap<String, Object>();
        createConnectionFactories(environmentCopy, bindings);
        createQueues(environmentCopy, bindings);
        createTopics(environmentCopy, bindings);

        return new ReadOnlyContext(environmentCopy, bindings);
    }

    private void createConnectionFactories(Hashtable<Object, Object> environment, Map<String, Object> bindings) throws NamingException {
        Map<String, String> factories = getConnectionFactoryNamesAndURIs(environment);
        Map<String, String> defaults = getConnectionFactoryDefaults(environment);
        for (Map.Entry<String, String> entry : factories.entrySet()) {
            String name = entry.getKey();
            String uri = entry.getValue();

            PulsarConnectionFactory factory = null;
            try {
                factory = createConnectionFactory(name, uri, defaults, environment);
            } catch (Exception e) {
                NamingException ne = new NamingException("Exception while creating ConnectionFactory '" + name + "'.");
                ne.initCause(e);
                throw ne;
            }

            bindings.put(name, factory);
        }
    }

    protected Map<String, String> getConnectionFactoryNamesAndURIs(Map<Object, Object> environment) {
        Map<String, String> factories = new LinkedHashMap<String, String>();
        for (Iterator<Map.Entry<Object, Object>> iter = environment.entrySet().iterator(); iter.hasNext();) {
            Map.Entry<Object, Object> entry = iter.next();
            String key = String.valueOf(entry.getKey());
            if (key.toLowerCase().startsWith(CONNECTION_FACTORY_KEY_PREFIX)) {
                String factoryName = key.substring(CONNECTION_FACTORY_KEY_PREFIX.length());
                String value = null;
                if(entry.getValue() != null) {
                    value = String.valueOf(entry.getValue());
                }

                factories.put(factoryName, value);
            }
        }

        if (factories.isEmpty()) {
            for (int i = 0; i < DEFAULT_CONNECTION_FACTORY_NAMES.length; i++) {
                factories.put(DEFAULT_CONNECTION_FACTORY_NAMES[i], null);
            }
        }

        return factories;
    }

    protected Map<String, String> getConnectionFactoryDefaults(Map<Object, Object> environment) {
        Map<String, String> map = new LinkedHashMap<String, String>();

        for (Iterator<Map.Entry<Object, Object>> iter = environment.entrySet().iterator(); iter.hasNext();) {
            Map.Entry<Object, Object> entry = iter.next();
            String key = String.valueOf(entry.getKey());
            if (key.toLowerCase().startsWith(CONNECTION_FACTORY_DEFAULT_KEY_PREFIX)) {
                String jndiName = key.substring(CONNECTION_FACTORY_DEFAULT_KEY_PREFIX.length());
                map.put(jndiName, String.valueOf(entry.getValue()));
            }
        }

        return Collections.unmodifiableMap(map);
    }

    protected Map<String, String> getConnectionFactoryProperties(String factoryName, Map<Object, Object> environment) {
        Map<String, String> map = new LinkedHashMap<String, String>();

        final String factoryNameSuffix = factoryName + ".";

        for (Iterator<Map.Entry<Object, Object>> iter = environment.entrySet().iterator(); iter.hasNext();) {
            Map.Entry<Object, Object> entry = iter.next();
            String key = String.valueOf(entry.getKey());
            if (key.toLowerCase().startsWith(CONNECTION_FACTORY_PROPERTY_KEY_PREFIX)) {
                if(key.substring(CONNECTION_FACTORY_PROPERTY_KEY_PREFIX.length()).startsWith(factoryNameSuffix)) {
                    String propertyName = key.substring(CONNECTION_FACTORY_PROPERTY_KEY_PREFIX.length() + factoryNameSuffix.length());
                    map.put(propertyName, String.valueOf(entry.getValue()));
                }
            }
        }

        return map;
    }

    protected PulsarConnectionFactory createConnectionFactory(String name, String uri, Map<String, String> defaults, Hashtable<Object, Object> environment) throws URISyntaxException {
        Map<String, String> props = new LinkedHashMap<String, String>();

        // Add the defaults which apply to all connection factories
        props.putAll(defaults);

        // Add any URI entry for this specific factory name
        if (uri != null && !uri.trim().isEmpty()) {
            props.put(PulsarConnectionFactory.DEFAULT_BROKER_PROP, uri);
        }

        // Add any factory-specific additional properties
        props.putAll(getConnectionFactoryProperties(name, environment));

        return createConnectionFactory(props);
    }

    /**
     * Factory method to create a new connection factory using the given properties
     */
    protected PulsarConnectionFactory createConnectionFactory(Map<String, String> properties) {
        PulsarConnectionFactory factory = new PulsarConnectionFactory();
        Map<String, String> unused = factory.setProperties(properties);
        if (!unused.isEmpty()) {
            String msg =
                    " Not all properties could be set on the ConnectionFactory."
                            + " Check the properties are spelled correctly."
                            + " Unused properties=[" + unused + "].";
            throw new IllegalArgumentException(msg);
        }

        return factory;
    }

    protected void createQueues(Hashtable<Object, Object> environment, Map<String, Object> bindings) {
        for (Iterator<Map.Entry<Object, Object>> iter = environment.entrySet().iterator(); iter.hasNext(); ) {
            Map.Entry<Object, Object> entry = iter.next();
            String key = entry.getKey().toString();
            if (key.startsWith(QUEUE_KEY_PREFIX)) {
                String jndiName = key.substring(QUEUE_KEY_PREFIX.length());
                bindings.put(jndiName, createQueue(entry.getValue().toString()));
            }
        }
    }

    protected void createTopics(Hashtable<Object, Object> environment, Map<String, Object> bindings) {
        for (Iterator<Map.Entry<Object, Object>> iter = environment.entrySet().iterator(); iter.hasNext(); ) {
            Map.Entry<Object, Object> entry = iter.next();
            String key = entry.getKey().toString();
            if (key.startsWith(TOPIC_KEY_PREFIX)) {
                String jndiName = key.substring(TOPIC_KEY_PREFIX.length());
                bindings.put(jndiName, createTopic(entry.getValue().toString()));
            }
        }
    }

    /**
     * Factory method to create new Queue instances
     */
    protected Queue createQueue(String name) {
        return new PulsarQueue(name);
    }

    /**
     * Factory method to create new Topic instances
     */
    protected Topic createTopic(String name) {
        return new PulsarTopic(name);
    }
}
