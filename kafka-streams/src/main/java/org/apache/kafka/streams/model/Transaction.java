package org.apache.kafka.streams.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Date;
import java.util.Objects;

/**
 * @author arthur
 */
public class Transaction {
    private String firstName;
    private String lastName;
    private String customerId;
    private String creditCardNumber;
    private String itemPurchased;
    private String department;
    private int quantity;
    private double price;
    private Date purchaseDate;
    private String zipCode;
    public Transaction() {
    }
    public Transaction(Builder builder) {
        this.firstName=builder.firstName;
        this.lastName=builder.lastName;
        this.customerId=builder.customerId;
        this.creditCardNumber=builder.creditCardNumber;
        this.itemPurchased=builder.itemPurchased;
        this.department=builder.department;
        this.quantity=builder.quantity;
        this.price=builder.price;
        this.purchaseDate=builder.purchaseDate;
        this.zipCode=builder.zipCode;
    }

    public static Builder newBuilder(){
        return new Builder();
    }

    public static Builder builder(Transaction transaction){
        Builder builder = new Builder();
        builder.firstName=transaction.firstName;
        builder.lastName=transaction.lastName;
        builder.customerId=transaction.customerId;
        builder.creditCardNumber=transaction.creditCardNumber;
        builder.itemPurchased=transaction.itemPurchased;
        builder.department=transaction.department;
        builder.quantity=transaction.quantity;
        builder.price=transaction.price;
        builder.purchaseDate=transaction.purchaseDate;
        builder.zipCode=transaction.zipCode;
        return builder;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getCreditCardNumber() {
        return creditCardNumber;
    }

    public void setCreditCardNumber(String creditCardNumber) {
        this.creditCardNumber = creditCardNumber;
    }

    public String getItemPurchased() {
        return itemPurchased;
    }

    public void setItemPurchased(String itemPurchased) {
        this.itemPurchased = itemPurchased;
    }

    public String getDepartment() {
        return department;
    }

    public void setDepartment(String department) {
        this.department = department;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public Date getPurchaseDate() {
        return purchaseDate;
    }

    public void setPurchaseDate(Date purchaseDate) {
        this.purchaseDate = purchaseDate;
    }

    public String getZipCode() {
        return zipCode;
    }

    public void setZipCode(String zipCode) {
        this.zipCode = zipCode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Transaction that = (Transaction) o;
        return quantity == that.quantity && Double.compare(that.price, price) == 0 && Objects.equals(firstName, that.firstName) && Objects.equals(lastName, that.lastName) && Objects.equals(customerId, that.customerId) && Objects.equals(creditCardNumber, that.creditCardNumber) && Objects.equals(itemPurchased, that.itemPurchased) && Objects.equals(department, that.department) && Objects.equals(purchaseDate, that.purchaseDate) && Objects.equals(zipCode, that.zipCode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(firstName, lastName, customerId, creditCardNumber, itemPurchased, department, quantity, price, purchaseDate, zipCode);
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", customerId='" + customerId + '\'' +
                ", creditCardNumber='" + creditCardNumber + '\'' +
                ", itemPurchased='" + itemPurchased + '\'' +
                ", department='" + department + '\'' +
                ", quantity=" + quantity +
                ", price=" + price +
                ", purchaseDate=" + purchaseDate +
                ", zipCode='" + zipCode + '\'' +
                '}';
    }

    public static class Builder{
        private String firstName;
        private String lastName;
        private String customerId;
        private String creditCardNumber;
        private String itemPurchased;
        private String department;
        private int quantity;
        private double price;
        private Date purchaseDate;
        private String zipCode;
        private static final String MASKING_CREDIT_CARD="xxx-xxxx-xxxx-";

        private Builder(){

        }

        public Builder maskCreditCard(){
            Objects.requireNonNull(this.creditCardNumber,"Credit card number can't be null");
            String[] parts = this.creditCardNumber.split("-");
            if(parts.length<4){
                this.creditCardNumber="xxx";
            }else {
                final String last4Digits = parts[3];
                this.creditCardNumber = MASKING_CREDIT_CARD + last4Digits;
            }
            return this;
        }

        public Builder firstName(String firstName){
            this.firstName=firstName;
            return this;
        }
        public Builder lastName(String lastName){
            this.lastName=lastName;
            return this;
        }
        public Builder customerId(String customerId){
            this.customerId=customerId;
            return this;
        }
        public Builder creditCardNumber(String creditCardNumber){
            this.creditCardNumber=creditCardNumber;
            return this;
        }
        public Builder itemPurchased(String itemPurchased){
            this.itemPurchased=itemPurchased;
            return this;
        }
        public Builder department(String department){
            this.department=department;
            return this;
        }
        public Builder quantity(int quantity){
            this.quantity=quantity;
            return this;
        }
        public Builder price(double price){
            this.price=price;
            return this;
        }
        public Builder purchaseDate(Date purchaseDate){
            this.purchaseDate=purchaseDate;
            return this;
        }
        public Builder zipCode(String zipCode){
            this.zipCode=zipCode;
            return this;
        }

        public Transaction build(){
            return new Transaction(this);
        }
    }

    public static void main(String[] args) throws JsonProcessingException {
        Transaction transaction = Transaction.newBuilder()
                .firstName("Arthur")
                .lastName("Dent")
                .customerId("123456")
                .creditCardNumber("1234-5678-9012-3456")
                .itemPurchased("Donut")
                .department("Food")
                .quantity(1)
                .price(1.99)
                .purchaseDate(new Date())
                .zipCode("12345")
                .build();
        ObjectMapper objectMapper=new ObjectMapper();
        System.out.println(objectMapper.writeValueAsString(transaction));
    }
}
