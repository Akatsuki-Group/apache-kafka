package org.apache.kafka.streams.model;

import java.util.Objects;

/**
 * @author arthur
 */
public class SaleState {
    private String department;
    private double totalAmount;
    private double saleAmount;
    private double averageAmount;
    private int count;

    public SaleState() {
    }

    public String getDepartment() {
        return department;
    }

    public void setDepartment(String department) {
        this.department = department;
    }

    public double getTotalAmount() {
        return totalAmount;
    }

    public double getSaleAmount() {
        return saleAmount;
    }

    public void setSaleAmount(double saleAmount) {
        this.saleAmount = saleAmount;
    }

    public void setTotalAmount(double totalAmount) {
        this.totalAmount = totalAmount;
    }

    public double getAverageAmount() {
        return averageAmount;
    }

    public void setAverageAmount(double averageAmount) {
        this.averageAmount = averageAmount;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SaleState saleState = (SaleState) o;
        return Double.compare(saleState.totalAmount, totalAmount) == 0 && Double.compare(saleState.averageAmount, averageAmount) == 0 && count == saleState.count && Objects.equals(department, saleState.department);
    }

    @Override
    public int hashCode() {
        return Objects.hash(department, totalAmount, averageAmount, count);
    }

    @Override
    public String toString() {
        return "SaleState{" +
                "department='" + department + '\'' +
                ", totalAmount=" + totalAmount +
                ", averageAmount=" + averageAmount +
                ", count=" + count +
                '}';
    }
}
