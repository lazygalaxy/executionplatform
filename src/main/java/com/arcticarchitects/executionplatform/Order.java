package com.arcticarchitects.executionplatform;

public class Order {
    public char side;
    public char ordType;
    public int quantity;
    public double price;
    public String symbol;
    public String currency;
    public String securityExchange;
    public double lastPx;
    public String execBroker;

    public Order(char side, char ordType, int quantity, double price, String symbol,
            String currency, String securityExchange, double lastPx, String execBroker) {
        this.side = side;
        this.ordType = ordType;
        this.quantity = quantity;
        this.price = price;
        this.symbol = symbol;
        this.currency = currency;
        this.securityExchange = securityExchange;
        this.lastPx = lastPx;
        this.execBroker = execBroker;
    }
}
