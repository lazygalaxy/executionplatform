package com.arcticdatascape.executionplatform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.snowflake.ingest.internal.apache.commons.math3.util.Pair;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import quickfix.field.ExecType;
import quickfix.field.OrdStatus;
import quickfix.field.OrdType;
import quickfix.field.Side;
import quickfix.fix42.ExecutionReport;
import quickfix.fix42.Message;

public class SnowpipeStream {
    private static final Logger logger = LogManager.getLogger(FixMsgCreate.class);
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss.SSS");
    private final Map<Integer, Pair<String, Character>> tagToNameMap = new HashMap<>();

    // Open a streaming ingest channel from the given client
    private final SnowflakeStreamingIngestChannel ordersChannel;
    private final SnowflakeStreamingIngestChannel tradesChannel;

    SnowpipeStream() throws Exception {

        tagToNameMap.put(1, new Pair<String, Character>("Account", ExecType.NEW));
        tagToNameMap.put(6, new Pair<String, Character>("AvgPx", ExecType.NEW));
        tagToNameMap.put(11, new Pair<String, Character>("ClOrdID", ExecType.NEW));
        tagToNameMap.put(14, new Pair<String, Character>("CumQty", ExecType.NEW));
        tagToNameMap.put(15, new Pair<String, Character>("Currency", ExecType.NEW));
        tagToNameMap.put(17, new Pair<String, Character>("ExecID", ExecType.TRADE));
        tagToNameMap.put(30, new Pair<String, Character>("LastMkt", ExecType.TRADE));
        tagToNameMap.put(31, new Pair<String, Character>("LastPx", ExecType.TRADE));
        tagToNameMap.put(32, new Pair<String, Character>("LastShares", ExecType.TRADE));
        tagToNameMap.put(37, new Pair<String, Character>("OrderID", null));
        tagToNameMap.put(38, new Pair<String, Character>("OrderQty", ExecType.NEW));
        tagToNameMap.put(39, new Pair<String, Character>("OrdStatus", ExecType.NEW));
        tagToNameMap.put(40, new Pair<String, Character>("OrdType", ExecType.NEW));
        tagToNameMap.put(44, new Pair<String, Character>("Price", ExecType.NEW));
        tagToNameMap.put(49, new Pair<String, Character>("SenderCompID", ExecType.NEW));
        tagToNameMap.put(52, new Pair<String, Character>("SendingTime", null));
        tagToNameMap.put(54, new Pair<String, Character>("Side", ExecType.NEW));
        tagToNameMap.put(55, new Pair<String, Character>("Symbol", ExecType.NEW));
        tagToNameMap.put(60, new Pair<String, Character>("TransactTime", null));
        tagToNameMap.put(76, new Pair<String, Character>("ExecBroker", ExecType.TRADE));
        tagToNameMap.put(151, new Pair<String, Character>("LeavesQty", ExecType.NEW));
        tagToNameMap.put(207, new Pair<String, Character>("SecurityExchange", ExecType.NEW));

        Iterator<Map.Entry<String, JsonNode>> propIt = new ObjectMapper()
                .readTree(getClass().getClassLoader().getResourceAsStream("profile.json")).fields();

        Properties props = new Properties();
        while (propIt.hasNext()) {
            Map.Entry<String, JsonNode> prop = propIt.next();
            props.put(prop.getKey(), prop.getValue().asText());
        }

        SnowflakeStreamingIngestClient client = SnowflakeStreamingIngestClientFactory.builder("EXECUTION_PLATFORM")
                .setProperties(props).build();

        // Create an open channel request on table MY_TABLE, note that the corresponding
        // db/schema/table needs to be present
        // Example: create or replace table MY_TABLE(c1 number);
        OpenChannelRequest ordersRequest = OpenChannelRequest.builder("ORDER_CHANNEL")
                .setDBName("PULO1")
                .setSchemaName("INTERNALFLOW")
                .setTableName("TBLORDERS")
                .setOnErrorOption(
                        OpenChannelRequest.OnErrorOption.CONTINUE) // Another ON_ERROR option is ABORT
                .build();

        ordersChannel = client.openChannel(ordersRequest);

        OpenChannelRequest tradesRequest = OpenChannelRequest.builder("TRADE_CHANNEL")
                .setDBName("PULO1")
                .setSchemaName("INTERNALFLOW")
                .setTableName("TBLTRADES")
                .setOnErrorOption(
                        OpenChannelRequest.OnErrorOption.CONTINUE) // Another ON_ERROR option is ABORT
                .build();

        tradesChannel = client.openChannel(tradesRequest);
    }

    public void insert(ExecutionReport executionReport) throws Exception {

        switch (executionReport.getExecType().getValue()) {
            case ExecType.NEW:
                insertRow(ordersChannel, getRow(executionReport, ExecType.NEW));
                break;
            case ExecType.TRADE:
                insertRow(tradesChannel, getRow(executionReport, ExecType.TRADE));
                insertRow(ordersChannel, getRow(executionReport, ExecType.NEW));
                break;
        }

    }

    private void insertRow(SnowflakeStreamingIngestChannel channel, Map<String, Object> row) {
        InsertValidationResponse response = channel.insertRow(row, null);
        if (response.hasErrors())
            throw response.getInsertErrors().get(0).getException();
    }

    private Map<String, Object> getRow(ExecutionReport fixMessage, Character execType) throws Exception {
        Map<String, Object> row = new HashMap<String, Object>();
        String[] fields = fixMessage.toString().split("\001");

        for (String field : fields) {
            String[] pair = field.split("=");
            Pair<String, Character> info = tagToNameMap.get(Integer.parseInt(pair[0]));
            if (info != null && (info.getSecond() == null || execType.equals(info.getSecond()))) {
                String label = info.getFirst().toUpperCase();
                String value = pair[1];
                if (label.endsWith("TIME"))
                    row.put(info.getFirst(), LocalDateTime.parse(value, formatter));
                else if (label.equals("SIDE"))
                    row.put(info.getFirst(), fixMessage.getSide().getValue() == Side.BUY ? "buy" : "sell");
                else if (label.equals("ORDTYPE"))
                    row.put(info.getFirst(), fixMessage.getOrdType().getValue() == OrdType.LIMIT ? "limit" : "market");
                else if (label.equals("ORDSTATUS"))
                    row.put(info.getFirst(), fixMessage.getOrdStatus().getValue() == OrdStatus.NEW ? "new"
                            : fixMessage.getOrdStatus().getValue() == OrdStatus.FILLED ? "filled" : "partially_filled");
                else if ((label.equals("PRICE") || label.equals("AVGPX"))
                        && (value == null || value.equals("0") || value
                                .equals("0.00")))
                    continue;
                else
                    row.put(info.getFirst(), value);
            }
        }

        return row;
    }

    public void close() throws Exception {
        ordersChannel.close().get();
        tradesChannel.close().get();
    }
}