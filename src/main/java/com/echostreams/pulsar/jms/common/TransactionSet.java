package com.echostreams.pulsar.jms.common;

import com.echostreams.pulsar.jms.queue.PulsarQueue;
import com.echostreams.pulsar.jms.utils.PulsarJMSException;

import javax.jms.JMSException;
import javax.jms.Session;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * <p>
 * Set of {@link TransactionItem} objects.
 * Used by the JMS {@link Session} implementation to keep track of the changes made in the current transaction.
 * May be fully or partially cleared on commit/rollback operations.
 * Thread-safe implementation.
 * </p>
 */
public final class TransactionSet {
    private LinkedList<TransactionItem> items = new LinkedList<>();

    public TransactionSet() {
        super();
    }

    public synchronized void add(int handle, String messageID, int deliveryMode, PulsarQueue destination) {
        // Create a new transaction item
        TransactionItem item = new TransactionItem(handle,
                messageID,
                deliveryMode,
                destination);
        items.add(item);
    }

    public synchronized void add(TransactionItem item) {
        items.add(item);
    }

    public synchronized void removeUpdatesForQueue(String queueName) throws JMSException {
        Iterator<TransactionItem> entries = items.iterator();
        while (entries.hasNext()) {
            TransactionItem item = entries.next();
            if (item.getDestination().getQueueName().equals(queueName))
                entries.remove();
        }
    }

    public synchronized int size() {
        return items.size();
    }

    /**
     * Clear items by IDs from the transaction set and return a snapshot of the items
     */
    public synchronized TransactionItem[] clear(List<String> deliveredMessageIDs) throws PulsarJMSException {
        int len = deliveredMessageIDs.size();
        TransactionItem[] itemsSnapshot = new TransactionItem[len];
        for (int n = 0; n < len; n++) {
            String deliveredMessageID = deliveredMessageIDs.get(len - n - 1);

            boolean found = false;
            Iterator<TransactionItem> entries = items.iterator();
            while (entries.hasNext()) {
                TransactionItem item = entries.next();
                if (item.getMessageId().equals(deliveredMessageID)) {
                    found = true;
                    itemsSnapshot[n] = item; // Store in snapshot
                    entries.remove();
                    break;
                }
            }

            if (!found)
                throw new PulsarJMSException("Message does not belong to transaction : " + deliveredMessageID, "INTERNAL_ERROR");
        }
        return itemsSnapshot;
    }

    /**
     * Clear the set and return a snapshot of its content
     */
    public synchronized TransactionItem[] clear() {
        // Create snapshot
        TransactionItem[] itemsSnapshot = items.toArray(new TransactionItem[items.size()]);

        // Clear
        items.clear();

        return itemsSnapshot;
    }

    /**
     * Compute a list of queues that were updated in this transaction set
     */
    public synchronized List<PulsarQueue> updatedQueues() {
        List<PulsarQueue> updatedQueues = new ArrayList<>(items.size());
        for (int i = 0; i < items.size(); i++) {
            TransactionItem item = items.get(i);
            PulsarQueue pulsarQueue = item.getDestination();
            if (!updatedQueues.contains(pulsarQueue))
                updatedQueues.add(pulsarQueue);
        }

        return updatedQueues;
    }

    /**
     * Compute a list of queues that were updated in this transaction set
     */
    public synchronized List<PulsarQueue> updatedQueues(List<String> deliveredMessageIDs) throws PulsarJMSException {
        int len = deliveredMessageIDs.size();
        List<PulsarQueue> updatedQueues = new ArrayList<>(len);
        for (int n = 0; n < len; n++) {
            String deliveredMessageID = deliveredMessageIDs.get(len - n - 1);

            boolean found = false;
            Iterator<TransactionItem> entries = items.iterator();
            while (entries.hasNext()) {
                TransactionItem item = entries.next();
                if (item.getMessageId().equals(deliveredMessageID)) {
                    found = true;

                    PulsarQueue pulsarQueue = item.getDestination();
                    if (!updatedQueues.contains(pulsarQueue))
                        updatedQueues.add(pulsarQueue);

                    break;
                }
            }

            if (!found)
                throw new PulsarJMSException("Message does not belong to transaction : " + deliveredMessageID, "INTERNAL_ERROR");
        }
        return updatedQueues;
    }
}
