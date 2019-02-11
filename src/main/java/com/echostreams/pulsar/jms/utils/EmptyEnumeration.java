package com.echostreams.pulsar.jms.utils;

import java.util.Enumeration;
import java.util.NoSuchElementException;

public final class EmptyEnumeration<T> implements Enumeration<T>
{
	public EmptyEnumeration()
	{
		super();
	}

	@Override
	public boolean hasMoreElements()
	{
		return false;
	}

	@Override
	public T nextElement()
	{
		throw new NoSuchElementException();
	}
}
