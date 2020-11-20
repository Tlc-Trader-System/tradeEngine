package com.tradeengine.tradeengine;

public class OrderBookRequest {
    public String id;
    public String product;
    public String side;

    public OrderBookRequest() {
    }

    public OrderBookRequest(String id, String product, String side) {
        this.id = id;
        this.product = product;
        this.side = side;
    }
}
