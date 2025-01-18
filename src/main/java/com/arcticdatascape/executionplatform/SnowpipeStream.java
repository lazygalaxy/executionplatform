package com.arcticdatascape.executionplatform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import quickfix.fix44.Message;

public class SnowpipeStream {
    private static final ObjectMapper mapper = new ObjectMapper();

    // Open a streaming ingest channel from the given client
    SnowflakeStreamingIngestClient client;
    SnowflakeStreamingIngestChannel channel;

    SnowpipeStream() throws Exception {

        Iterator<Map.Entry<String, JsonNode>> propIt = mapper
                .readTree(getClass().getClassLoader().getResourceAsStream("profile.json")).fields();

        Properties props = new Properties();
        while (propIt.hasNext()) {
            Map.Entry<String, JsonNode> prop = propIt.next();
            props.put(prop.getKey(), prop.getValue().asText());
        }

        client = SnowflakeStreamingIngestClientFactory.builder("MY_CLIENT").setProperties(props).build();

        // Create an open channel request on table MY_TABLE, note that the corresponding
        // db/schema/table needs to be present
        // Example: create or replace table MY_TABLE(c1 number);
        OpenChannelRequest request = OpenChannelRequest.builder("MY_CHANNEL")
                .setDBName("PULO1")
                .setSchemaName("INTERNALFLOW")
                .setTableName("FIXMESSAGE")
                .setOnErrorOption(
                        OpenChannelRequest.OnErrorOption.CONTINUE) // Another ON_ERROR option is ABORT
                .build();

        channel = client.openChannel(request);
    }

    public void insert(Message fixMessage) throws Exception {
        Map<String, Object> row = new HashMap<String, Object>();
        row.put("fix", fixMessage.toString());
        // Insert the row with the current offset_token
        InsertValidationResponse response = channel.insertRow(row, null);
        if (response.hasErrors()) {
            // Simply throw if there is an exception, or you can do whatever you want with
            // the erroneous row
            throw response.getInsertErrors().get(0).getException();
        }
    }

    public void close() throws Exception {
        channel.close().get();
    }
}