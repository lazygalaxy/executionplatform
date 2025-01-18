package com.arcticdatascape.executionplatform;

import quickfix.field.OrdType;
import quickfix.field.Side;
import quickfix.fix44.ExecutionReport;
import quickfix.fix44.NewOrderSingle;

public class App {
    public static void main(String[] args) throws Exception {
        FixMsgCreate fixMsgCreate = new FixMsgCreate();
        SnowpipeStream snowpipeStream = new SnowpipeStream();
        // Simulate creating an order
        NewOrderSingle newOrderSingle = fixMsgCreate.newOrderSingle(Side.BUY, OrdType.LIMIT, 1000, 150.50,
                "CH0038863350", "CHF", "XSWX", "101010.001");
        snowpipeStream.insert(newOrderSingle);
        ExecutionReport executionReportNew = fixMsgCreate.executionReportNew(newOrderSingle);
        snowpipeStream.insert(executionReportNew);

        snowpipeStream.close();
    }
}
