package com.echostreams.pulsar.jms.jndi;

import com.echostreams.pulsar.jms.client.PulsarConnectionFactory;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;
import javax.naming.StringRefAddr;
import javax.naming.spi.ObjectFactory;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;

public class JNDIObjectFactory implements ObjectFactory {
    /**
     * Creates an object using the location or reference information
     * specified.
     * <p>
     * Special requirements of this object are supplied
     * using <code>environment</code>.
     * An example of such an environment property is user identity
     * information.
     * <p>
     * <tt>NamingManager.getObjectInstance()</tt>
     * successively loads in object factories and invokes this method
     * on them until one produces a non-null answer.  When an exception
     * is thrown by an object factory, the exception is passed on to the caller
     * of <tt>NamingManager.getObjectInstance()</tt>
     * (and no search is made for other factories
     * that may produce a non-null answer).
     * An object factory should only throw an exception if it is sure that
     * it is the only intended factory and that no other object factories
     * should be tried.
     * If this factory cannot create an object using the arguments supplied,
     * it should return null.
     * <p>
     * A <em>URL context factory</em> is a special ObjectFactory that
     * creates contexts for resolving URLs or objects whose locations
     * are specified by URLs.  The <tt>getObjectInstance()</tt> method
     * of a URL context factory will obey the following rules.
     * <ol>
     * <li>If <code>obj</code> is null, create a context for resolving URLs of the
     * scheme associated with this factory. The resulting context is not tied
     * to a specific URL:  it is able to handle arbitrary URLs with this factory's
     * scheme id.  For example, invoking <tt>getObjectInstance()</tt> with
     * <code>obj</code> set to null on an LDAP URL context factory would return a
     * context that can resolve LDAP URLs
     * such as "ldap://ldap.wiz.com/o=wiz,c=us" and
     * "ldap://ldap.umich.edu/o=umich,c=us".
     * <li>
     * If <code>obj</code> is a URL string, create an object (typically a context)
     * identified by the URL.  For example, suppose this is an LDAP URL context
     * factory.  If <code>obj</code> is "ldap://ldap.wiz.com/o=wiz,c=us",
     * getObjectInstance() would return the context named by the distinguished
     * name "o=wiz, c=us" at the LDAP server ldap.wiz.com.  This context can
     * then be used to resolve LDAP names (such as "cn=George")
     * relative to that context.
     * <li>
     * If <code>obj</code> is an array of URL strings, the assumption is that the
     * URLs are equivalent in terms of the context to which they refer.
     * Verification of whether the URLs are, or need to be, equivalent is up
     * to the context factory. The order of the URLs in the array is
     * not significant.
     * The object returned by getObjectInstance() is like that of the single
     * URL case.  It is the object named by the URLs.
     * <li>
     * If <code>obj</code> is of any other type, the behavior of
     * <tt>getObjectInstance()</tt> is determined by the context factory
     * implementation.
     * </ol>
     * <p>
     * <p>
     * The <tt>name</tt> and <tt>environment</tt> parameters
     * are owned by the caller.
     * The implementation will not modify these objects or keep references
     * to them, although it may keep references to clones or copies.
     * <p>
     * <p>
     * <b>Name and Context Parameters.</b> &nbsp;&nbsp;&nbsp;
     * <a name=NAMECTX></a>
     * <p>
     * The <code>name</code> and <code>nameCtx</code> parameters may
     * optionally be used to specify the name of the object being created.
     * <code>name</code> is the name of the object, relative to context
     * <code>nameCtx</code>.
     * If there are several possible contexts from which the object
     * could be named -- as will often be the case -- it is up to
     * the caller to select one.  A good rule of thumb is to select the
     * "deepest" context available.
     * If <code>nameCtx</code> is null, <code>name</code> is relative
     * to the default initial context.  If no name is being specified, the
     * <code>name</code> parameter should be null.
     * If a factory uses <code>nameCtx</code> it should synchronize its use
     * against concurrent access, since context implementations are not
     * guaranteed to be thread-safe.
     * <p>
     *
     * @param obj         The possibly null object containing location or reference
     *                    information that can be used in creating an object.
     * @param name        The name of this object relative to <code>nameCtx</code>,
     *                    or null if no name is specified.
     * @param nameCtx     The context relative to which the <code>name</code>
     *                    parameter is specified, or null if <code>name</code> is
     *                    relative to the default initial context.
     * @param environment The possibly null environment that is used in
     *                    creating the object.
     * @return The object created; null if an object cannot be created.
     * @throws Exception if this object factory encountered an exception
     *                   while attempting to create an object, and no other object factories are
     *                   to be tried.
     * @see NamingManager#getObjectInstance
     * @see NamingManager#getURLContext
     */
    @Override
    public Object getObjectInstance(Object obj, Name name, Context nameCtx, Hashtable<?, ?> environment) throws Exception {
        Object result = null;

        if (obj instanceof Reference) {
            Reference ref = (Reference) obj;
            String clazz = ref.getClassName();

            // ConnectionFactory or QueueConnectionFactory or TopicConnectionFactory
            if (clazz.equals(PulsarConnectionFactory.class.getName())) {

                StringRefAddr serverClass =
                        (StringRefAddr) ref.get("serverClass");
                String serverClassName = (String) serverClass.getContent();

                // get a list of string properties and store them in a
                // hashtable
                HashMap properties = new HashMap();
                Enumeration iter = ref.getAll();
                while (iter.hasMoreElements()) {
                    StringRefAddr addr = (StringRefAddr) iter.nextElement();
                    properties.put(addr.getType(), addr.getContent());
                }

                // create the factory
                //result = new PulsarConnectionFactory(serverClassName, properties, environment);
            }

        } else {
            throw new IllegalStateException("Object is not a reference : " + obj);
        }

        return result;
    }
}
