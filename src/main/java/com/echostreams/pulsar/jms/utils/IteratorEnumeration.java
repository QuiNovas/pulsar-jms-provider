package com.echostreams.pulsar.jms.utils;

import java.util.Enumeration;
import java.util.Iterator;

public final class IteratorEnumeration<T> implements Enumeration<T>
{
    private Iterator<T> iterator;

    public IteratorEnumeration( Iterator<T> iterator )
    {
        this.iterator = iterator;
    }

    @Override
	public boolean hasMoreElements()
    {
        return iterator.hasNext();
    }

    @Override
	public T nextElement()
    {
        return iterator.next();
    }
}
