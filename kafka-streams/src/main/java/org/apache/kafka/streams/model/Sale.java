package org.apache.kafka.streams.model;

import org.apache.kafka.streams.StreamsBuilder;

import java.util.Objects;

/**
 * @author arthur
 */
public class Sale {
    private String userName;
    private String department;
    private double saleAmount;
    private double totalSalesAmount;

    public Sale() {
    }

    public Sale(Builder builder) {
        this.userName = builder.userName;
        this.department = builder.department;
        this.saleAmount = builder.saleAmount;
        this.totalSalesAmount = builder.totalSalesAmount;
    }

    public static Builder newBuilder(Sale sale) {
        return new Builder(sale);
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getDepartment() {
        return department;
    }

    public void setDepartment(String department) {
        this.department = department;
    }

    public double getSaleAmount() {
        return saleAmount;
    }

    public void setSaleAmount(double saleAmount) {
        this.saleAmount = saleAmount;
    }

    public double getTotalSalesAmount() {
        return totalSalesAmount;
    }

    public void setTotalSalesAmount(double totalSalesAmount) {
        this.totalSalesAmount = totalSalesAmount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Sale sale = (Sale) o;
        return Double.compare(sale.saleAmount, saleAmount) == 0 && Double.compare(sale.totalSalesAmount, totalSalesAmount) == 0 && Objects.equals(userName, sale.userName) && Objects.equals(department, sale.department);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userName, department, saleAmount, totalSalesAmount);
    }

    @Override
    public String toString() {
        return "Sale{" +
                "userName='" + userName + '\'' +
                ", department='" + department + '\'' +
                ", saleAmount=" + saleAmount +
                ", totalSalesAmount=" + totalSalesAmount +
                '}';
    }

    public static final class Builder {
        private String userName;
        private String department;
        private double saleAmount;
        private double totalSalesAmount;

        private Builder(Sale sale) {
            this.userName = sale.userName;
            this.department = sale.department;
            this.saleAmount = sale.saleAmount;
            this.totalSalesAmount = sale.totalSalesAmount;
        }

        public Builder username(String userName) {
            this.userName = userName;
            return this;
        }

        public Builder department(String department) {
            this.department = department;
            return this;
        }

        public Builder saleAmount(double saleAmount) {
            this.saleAmount = saleAmount;
            return this;
        }

        public Builder totalSalesAmount(double totalSalesAmount) {
            this.totalSalesAmount = totalSalesAmount;
            return this;
        }

        public Builder accumulateSalesAmount(double saleAmount) {
            this.totalSalesAmount += saleAmount;
            return this;
        }

        public Sale build() {
            Sale sale = new Sale();
            sale.setUserName(userName);
            sale.setDepartment(department);
            sale.setSaleAmount(saleAmount);
            sale.setTotalSalesAmount(totalSalesAmount);
            return sale;
        }


    }
}
