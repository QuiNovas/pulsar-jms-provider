package com.echostreams.pulsar.jms.utils.id;

public final class IntegerID {
    private int id;

    public IntegerID(int id) {
        this.id = id;
    }

    public int asInt() {
        return id;
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof IntegerID))
            return false;

        IntegerID other = (IntegerID) obj;

        return other.id == id;
    }

    @Override
    public String toString() {
        return Integer.toString(id);
    }
}
