package com.arcticarchitects.executionplatform.blocks;

public class Customer {
    public String senderCompID;
    public String account;
    public long latency;

    public Customer(String senderCompID, String account, long latency) {
        this.senderCompID = senderCompID;
        this.account = account;
        this.latency = latency;
    }
}
