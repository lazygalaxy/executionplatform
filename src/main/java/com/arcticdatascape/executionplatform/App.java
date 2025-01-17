package com.arcticdatascape.executionplatform;

import quickfix.field.OrdType;
import quickfix.field.Side;
import quickfix.fix44.NewOrderSingle;

public class App {
    public static void main(String[] args) throws Exception {
        FixApplication app = new FixApplication();
        // Simulate creating an order
        NewOrderSingle newOrderSingle = app.createNewOrder(Side.BUY, OrdType.LIMIT, "AAPL", 1000, 150.50);
        app.createExecutionReportNew(newOrderSingle);
    }
}
