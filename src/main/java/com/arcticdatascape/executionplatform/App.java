package com.arcticdatascape.executionplatform;

import quickfix.field.OrdType;
import quickfix.field.Side;
import quickfix.fix42.ExecutionReport;

public class App {
    public static void main(String[] args) throws Exception {
        FixMsgCreate fixMsgCreate = new FixMsgCreate();
        SnowpipeStream snowpipeStream = new SnowpipeStream();

        ExecutionReport executionReportNew = fixMsgCreate.executionReportNew(Side.BUY, OrdType.LIMIT, 1000, 150.50,
                "CH0038863350", "CHF", "XSWX", "BLOOMBERG", "101010.001");
        snowpipeStream.insert(executionReportNew);

        ExecutionReport executionReportTrade = fixMsgCreate.executionReportTrade(executionReportNew, "DMA", 150.50);
        snowpipeStream.insert(executionReportTrade);

        executionReportNew = fixMsgCreate.executionReportNew(Side.SELL, OrdType.MARKET, 2000, 0.0,
                "CH0038863350", "CHF", "XSWX", "BLOOMBERG", "101010.001");
        snowpipeStream.insert(executionReportNew);

        executionReportTrade = fixMsgCreate.executionReportTrade(executionReportNew, "VIRTU", 150.50);
        snowpipeStream.insert(executionReportTrade);

        snowpipeStream.close();
    }
}
