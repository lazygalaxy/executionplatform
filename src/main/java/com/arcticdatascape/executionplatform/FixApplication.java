package com.arcticdatascape.executionplatform;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.UUID;

import quickfix.field.*;
import quickfix.fix44.ExecutionReport;
import quickfix.fix44.NewOrderSingle;

public class FixApplication {

    private static final Logger logger = LogManager.getLogger(FixApplication.class);

    // Method to generate New Order Single message
    public NewOrderSingle createNewOrder(char side, char ordType, String symbol, int quantity, double price)
            throws Exception {
        NewOrderSingle order = new NewOrderSingle(new ClOrdID(UUID.randomUUID().toString()),
                new Side(side), new TransactTime(LocalDateTime.now()), new OrdType(ordType));
        order.set(new Price(price));
        order.set(new OrderQty(quantity));
        order.set(new Symbol(symbol));
        order.set(new Currency("USD"));

        logger.info(order);
        return order;
    }

    // Method to generate Execution Report New
    public ExecutionReport createExecutionReportNew(NewOrderSingle order) throws Exception {
        ExecutionReport executionReportNew = new ExecutionReport();

        logger.info(executionReportNew);
        return executionReportNew;
    }
}
