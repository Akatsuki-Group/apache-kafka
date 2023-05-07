package org.apache.kafka.streams.model;

import java.util.Objects;

/**
 * @author arthur
 */
public class TransactionReward {
    private String customerId;
    private double purchaseTotal;
    private int rewardPoints;
    private int totalPoints;

    public TransactionReward(String customerId, double purchaseTotal, int rewardPoints) {
        this.customerId = customerId;
        this.purchaseTotal = purchaseTotal;
        this.rewardPoints = rewardPoints;
    }

    public static Builder builder(Transaction transaction){
        return new Builder(transaction);
    }

    public static Builder builder(TransactionReward transactionReward){
        return new Builder(transactionReward);
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public double getPurchaseTotal() {
        return purchaseTotal;
    }

    public void setPurchaseTotal(double purchaseTotal) {
        this.purchaseTotal = purchaseTotal;
    }

    public int getRewardPoints() {
        return rewardPoints;
    }

    public int getTotalPoints() {
        return totalPoints;
    }

    public void setRewardPoints(int rewardPoints) {
        this.rewardPoints = rewardPoints;
    }

    public void setTotalPoints(int totalPoints) {
        this.totalPoints = totalPoints;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransactionReward that = (TransactionReward) o;
        return Double.compare(that.purchaseTotal, purchaseTotal) == 0 && rewardPoints == that.rewardPoints && Objects.equals(customerId, that.customerId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(customerId, purchaseTotal, rewardPoints);
    }

    public static class Builder {
        private final String customerId;
        private final double purchaseTotal;
        private final int rewardPoints;


        public Builder(Transaction transaction) {
            this.customerId = transaction.getCustomerId();
            this.purchaseTotal = transaction.getPrice() * transaction.getQuantity();
            this.rewardPoints = (int) purchaseTotal;
        }

        public Builder(TransactionReward transactionReward) {
            this.customerId = transactionReward.getCustomerId();
            this.purchaseTotal = transactionReward.getPurchaseTotal();
            this.rewardPoints = transactionReward.getRewardPoints();
        }

        public TransactionReward build() {
            return new TransactionReward(this.customerId, this.purchaseTotal, this.rewardPoints);
        }
    }
}
