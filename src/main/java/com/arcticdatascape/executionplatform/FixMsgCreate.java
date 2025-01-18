package com.arcticdatascape.executionplatform;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.UUID;

import quickfix.field.*;
import quickfix.fix44.ExecutionReport;
import quickfix.fix44.Message;
import quickfix.fix44.NewOrderSingle;
import quickfix.fix44.Message.Header;

public class FixMsgCreate {

    private static final Logger logger = LogManager.getLogger(FixMsgCreate.class);

    // Method to generate New Order Single message
    public NewOrderSingle newOrderSingle(char side, char ordType, int quantity, double price, String isin,
            String currency, String securityExchange, String account)
            throws Exception {
        NewOrderSingle order = new NewOrderSingle(new ClOrdID(UUID.randomUUID().toString()),
                new Side(side), new TransactTime(LocalDateTime.now()), new OrdType(ordType));

        // order details
        order.set(new OrderQty(quantity));
        if (ordType == OrdType.LIMIT)
            order.set(new Price(price));

        // instrument details
        order.set(new Symbol(isin));
        order.set(new Currency(currency));
        order.set(new SecurityExchange(securityExchange));

        // client details
        order.set(new Account(account));

        logger.info(order);
        return order;
    }

    // Method to generate Execution Report New
    public ExecutionReport executionReportNew(NewOrderSingle order) throws Exception {
        ExecutionReport executionReportNew = new ExecutionReport(new OrderID(UUID.randomUUID().toString()),
                new ExecID(UUID.randomUUID().toString()), new ExecType(ExecType.NEW), new OrdStatus(OrdStatus.NEW),
                order.getSide(), new LeavesQty(order.getOrderQty().getValue()), new CumQty(0), new AvgPx(0));

        executionReportNew.set(order.getClOrdID());

        // order details
        executionReportNew.set(order.getSide());
        executionReportNew.set(order.getOrdType());
        executionReportNew.set(order.getOrderQty());
        if (order.getOrdType().valueEquals(OrdType.LIMIT))
            executionReportNew.set(order.getPrice());

        // instrument details
        executionReportNew.set(order.getSymbol());
        executionReportNew.set(order.getCurrency());
        executionReportNew.set(order.getSecurityExchange());

        // client details
        executionReportNew.set(order.getAccount());

        logger.info(executionReportNew);
        return executionReportNew;
    }
}
