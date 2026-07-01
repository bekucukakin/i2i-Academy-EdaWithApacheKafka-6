package com.i2iacademy.kafka.model;

import java.time.Instant;
import java.util.Objects;

public class OrderEvent {

    private String orderId;
    private String product;
    private int quantity;
    private double price;
    private String createdAt;

    public OrderEvent() {
        // Empty constructor required for Jackson deserialization
    }

    public OrderEvent(String orderId, String product, int quantity, double price) {
        this.orderId = orderId;
        this.product = product;
        this.quantity = quantity;
        this.price = price;
        this.createdAt = Instant.now().toString();
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
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

    public String getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OrderEvent)) return false;
        OrderEvent that = (OrderEvent) o;
        return quantity == that.quantity
                && Double.compare(price, that.price) == 0
                && Objects.equals(orderId, that.orderId)
                && Objects.equals(product, that.product);
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderId, product, quantity, price);
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "orderId='" + orderId + '\'' +
                ", product='" + product + '\'' +
                ", quantity=" + quantity +
                ", price=" + price +
                ", createdAt='" + createdAt + '\'' +
                '}';
    }
}