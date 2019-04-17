package com.echostreams.pulsar.jms.jndi;

import javax.naming.*;

public class NameParserImpl implements NameParser {
    /**
     * Parses a name into its components.
     *
     * @param name The non-null string name to parse.
     * @return A non-null parsed form of the name using the naming convention
     * of this parser.
     * @throws InvalidNameException If name does not conform to
     *                              syntax defined for the namespace.
     * @throws NamingException      If a naming exception was encountered.
     */
    @Override
    public Name parse(String name) throws NamingException {
        return new CompositeName(name);
    }
}
