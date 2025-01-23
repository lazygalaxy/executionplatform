package com.arcticarchitects.executionplatform.blocks;

public class Order {
    public char side;
    public char ordType;

    public Order(char side, char ordType) {
        this.side = side;
        this.ordType = ordType;
    }
}
