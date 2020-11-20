package com.tradeengine.tradeengine;

public class ValidatedOrder {
    public String id ;
    public String product;
    public String price;
    public String quantity;
    public String side;

    public ValidatedOrder() {
    }

    public ValidatedOrder(String id, String product, String price, String quantity, String side) {
        this.id = id;
        this.product = product;
        this.price = price;
        this.quantity = quantity;
        this.side = side;
    }
}
