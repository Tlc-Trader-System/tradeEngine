package com.tradeengine.tradeengine;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PendingOrder {
    public String product;
    public int quantity;
    public double price;
    public String side;
    public int cumulativeQuantity;
    public String exchange;

    public PendingOrder() {
    }

    public PendingOrder(String product, int quantity, double price, String side, int cumulativeQuantity, String exchange) {
        this.product = product;
        this.quantity = quantity;
        this.price = price;
        this.side = side;
        this.cumulativeQuantity = cumulativeQuantity;
        this.exchange = exchange;
    }

    @Override
    public String toString() {
        return "PendingOrder{" +
                "product='" + product + '\'' +
                ", quantity=" + quantity +
                ", price=" + price +
                ", side='" + side + '\'' +
                ", cumulativeQuantity=" + cumulativeQuantity +
                ", exchange='" + exchange + '\'' +
                '}';
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public String getExchange() {
        return exchange;
    }

    public void setExchange(String exchange) {
        this.exchange = exchange;
    }
}
