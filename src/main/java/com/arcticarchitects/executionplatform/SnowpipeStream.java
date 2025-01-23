package com.arcticarchitects.executionplatform;

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

import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import quickfix.field.OrdStatus;
import quickfix.field.OrdType;
import quickfix.field.Side;
import quickfix.fix42.ExecutionReport;

public class SnowpipeStream {
    private static final Logger logger = LogManager.getLogger(FixMsgCreate.class);

    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss.SSS");
    private final Map<Integer, String> tagToNameMap = new HashMap<>();

    // Open a streaming ingest channel from the given client
    private final SnowflakeStreamingIngestChannel ordersChannel;

    SnowpipeStream() throws Exception {

        tagToNameMap.put(1, "Customer");
        tagToNameMap.put(6, "AvgPx");
        tagToNameMap.put(11, "ClOrdID");
        tagToNameMap.put(14, "CumQty");
        tagToNameMap.put(15, "Currency");
        tagToNameMap.put(17, "ExecID");
        tagToNameMap.put(30, "LastMkt");
        tagToNameMap.put(31, "LastPx");
        tagToNameMap.put(32, "LastShares");
        tagToNameMap.put(37, "OrderID");
        tagToNameMap.put(38, "OrderQty");
        tagToNameMap.put(39, "OrdStatus");
        tagToNameMap.put(40, "OrdType");
        tagToNameMap.put(44, "Price");
        tagToNameMap.put(49, "SenderCompID");
        tagToNameMap.put(52, "SendingTime");
        tagToNameMap.put(54, "Side");
        tagToNameMap.put(55, "Symbol");
        tagToNameMap.put(60, "TransactTime");
        tagToNameMap.put(76, "ExecBroker");
        tagToNameMap.put(151, "LeavesQty");
        tagToNameMap.put(207, "SecurityExchange");

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
    }

    public void insert(ExecutionReport executionReport) throws Exception {
        insertRow(ordersChannel, getRow(executionReport));
    }

    private void insertRow(SnowflakeStreamingIngestChannel channel, Map<String, Object> row) {
        InsertValidationResponse response = channel.insertRow(row, null);
        if (response.hasErrors())
            throw response.getInsertErrors().get(0).getException();
    }

    private Map<String, Object> getRow(ExecutionReport fixMessage) throws Exception {
        Map<String, Object> row = new HashMap<String, Object>();
        String[] fields = fixMessage.toString().split("\001");

        for (String field : fields) {
            String[] pair = field.split("=");
            String label = tagToNameMap.get(Integer.parseInt(pair[0]));
            if (label != null) {
                label = label.toUpperCase();
                String value = pair[1];
                if (label.endsWith("TIME"))
                    row.put(label, LocalDateTime.parse(value, formatter));
                else if (label.equals("SIDE"))
                    row.put(label, fixMessage.getSide().getValue() == Side.BUY ? "buy" : "sell");
                else if (label.equals("ORDTYPE"))
                    row.put(label, fixMessage.getOrdType().getValue() == OrdType.LIMIT ? "limit" : "market");
                else if (label.equals("ORDSTATUS"))
                    row.put(label, fixMessage.getOrdStatus().getValue() == OrdStatus.NEW ? "new"
                            : fixMessage.getOrdStatus().getValue() == OrdStatus.FILLED ? "filled" : "partially_filled");
                else if ((label.equals("PRICE") || label.equals("AVGPX"))
                        && (value == null || value.equals("0") || value
                                .equals("0.00")))
                    continue;
                else
                    row.put(label, value);
            }
        }

        return row;
    }

    public void close() throws Exception {
        ordersChannel.close().get();
    }
}