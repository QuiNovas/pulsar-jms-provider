package com.echostreams.pulsar.jms.jndi;

import javax.naming.NameNotFoundException;
import javax.naming.NamingException;
import java.util.Hashtable;

public abstract class LazyCreateContext extends ReadOnlyContext {

    public LazyCreateContext(Hashtable<?, ?> environment) {
        super(environment);
    }

    public Object lookup(String name) throws NamingException {
        try {
            return super.lookup(name);
        } catch (NameNotFoundException e) {
            Object answer = createEntry(name);
            if (answer == null) {
                throw e;
            }
            internalBind(name, answer);
            return answer;
        }
    }

    protected abstract Object createEntry(String name);
}
