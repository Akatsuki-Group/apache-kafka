package org.apache.kafka.streams.model;

import java.util.Date;
import java.util.Objects;

/**
 * @author arthur
 */
public class TransactionKey {
    private final String customerId;
    private final Date transactionDate;

    public TransactionKey(String customerId, Date transactionDate) {
        this.customerId = customerId;
        this.transactionDate = transactionDate;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransactionKey that = (TransactionKey) o;
        return Objects.equals(customerId, that.customerId) && Objects.equals(transactionDate, that.transactionDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(customerId, transactionDate);
    }
}
