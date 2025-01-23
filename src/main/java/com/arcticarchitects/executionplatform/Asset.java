package com.arcticarchitects.executionplatform;

import quickfix.field.Side;

public class Asset {
    public String symbol;
    public String currency;
    public String securityExchange;
    public double price;
    public String execBroker;

    public Asset(String symbol, String currency, String securityExchange, double price, String execBroker) {
        this.symbol = symbol;
        this.currency = currency;
        this.securityExchange = securityExchange;
        this.price = price;
        this.execBroker = execBroker;
    }

    public double getLastPx(char side) {
        if (side == Side.BUY)
            return price - 0.05;
        return price + 0.05;
    }

}
