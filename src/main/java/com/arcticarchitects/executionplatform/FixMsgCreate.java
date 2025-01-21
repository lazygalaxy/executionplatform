package com.arcticarchitects.executionplatform;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.UUID;

import quickfix.field.*;
import quickfix.fix42.ExecutionReport;

public class FixMsgCreate {

    private static final Logger logger = LogManager.getLogger(FixMsgCreate.class);

    // Method to generate Execution Report New
    public ExecutionReport executionReportNew(char side, char ordType, int quantity, double price, String symbol,
            String currency, String securityExchange, String senderCompId, String clientAccount) throws Exception {
        ExecutionReport executionReportNew = new ExecutionReport(new OrderID(UUID.randomUUID().toString()),
                new ExecID(UUID.randomUUID().toString()), new ExecTransType(ExecTransType.NEW),
                new ExecType(ExecType.NEW), new OrdStatus(OrdStatus.NEW), new Symbol(symbol),
                new Side(side), new LeavesQty(quantity), new CumQty(0), new AvgPx(0));

        executionReportNew.set(new ClOrdID(UUID.randomUUID().toString()));

        // order details
        executionReportNew.set(new OrdType(ordType));
        executionReportNew.set(new OrderQty(quantity));
        if (executionReportNew.getOrdType().valueEquals(OrdType.LIMIT))
            executionReportNew.set(new Price(price));

        // instrument details
        executionReportNew.set(new Currency(currency));
        executionReportNew.set(new SecurityExchange(securityExchange));

        // client details
        executionReportNew.getHeader().setString(49, senderCompId);
        executionReportNew.set(new Account(clientAccount));

        executionReportNew.set(new TransactTime(LocalDateTime.now()));
        executionReportNew.getHeader().setUtcTimeStamp(52, LocalDateTime.now(), true);

        return executionReportNew;
    }

    // fully filled trade
    public ExecutionReport executionReportTrade(ExecutionReport executionReportNew, String execBroker, double lastPx)
            throws Exception {
        ExecutionReport executionReportTrade = (ExecutionReport) executionReportNew.clone();

        executionReportTrade.set(new ExecID(UUID.randomUUID().toString()));
        executionReportTrade.set(new ExecType(ExecType.TRADE));
        executionReportTrade.set(new OrdStatus(OrdStatus.FILLED));

        executionReportTrade.set(new ExecBroker(execBroker));

        executionReportTrade.set(new LastShares(executionReportNew.getOrderQty().getValue()));
        executionReportTrade.set(new LastPx(lastPx));
        executionReportTrade.set(new LastMkt(executionReportNew.getSecurityExchange().getValue()));

        executionReportTrade.set(new LeavesQty(0));
        executionReportTrade.set(new CumQty(executionReportNew.getOrderQty().getValue()));
        executionReportTrade.set(new AvgPx(lastPx));

        executionReportTrade.set(new TransactTime(LocalDateTime.now()));
        executionReportTrade.getHeader().setUtcTimeStamp(52, LocalDateTime.now(), true);

        return executionReportTrade;
    }

}
