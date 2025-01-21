package com.arcticarchitects.executionplatform;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import quickfix.field.OrdType;
import quickfix.field.Side;
import quickfix.fix42.ExecutionReport;

public class App {
        private static final Logger logger = LogManager.getLogger(FixMsgCreate.class);
        private static final Random random = new Random();

        private static final List<FixClient> fixClients = new ArrayList<FixClient>();
        private static final List<Order> orders = new ArrayList<Order>();

        public static void main(String[] args) throws Exception {
                fixClients.add(new FixClient("BLOOMBERG", "APLHA BANK"));
                fixClients.add(new FixClient("BLOOMBERG", "BETA BANK"));
                fixClients.add(new FixClient("BLOOMBERG", "GAMMA BANK"));
                fixClients.add(new FixClient("NYFIX", "DELTA BANK"));
                fixClients.add(new FixClient("NYFIX", "EPSILON BANK"));
                fixClients.add(new FixClient("AUTEX", "ZETA BANK"));

                orders.add(new Order(Side.BUY, OrdType.LIMIT, 1000, 74.20,
                                "NESN", "CHF", "XSWX", 74.10, "DMA"));
                orders.add(new Order(Side.SELL, OrdType.MARKET, 2000, 0.0,
                                "MSFT", "USD", "XNAS", 429.03, "BOFA"));

                FixMsgCreate fixMsgCreate = new FixMsgCreate();
                SnowpipeStream snowpipeStream = new SnowpipeStream();

                // while (System.in.available() == 0) {
                // Thread.sleep(1000);

                FixClient fixClient = fixClients.get(random.nextInt(fixClients.size()));
                Order order = orders.get(random.nextInt(orders.size()));

                ExecutionReport executionReportNew = fixMsgCreate.executionReportNew(order.side, order.ordType,
                                order.quantity, order.price, order.symbol, order.currency, order.securityExchange,
                                fixClient.senderCompID, fixClient.account);
                snowpipeStream.insert(executionReportNew);

                ExecutionReport executionReportTrade = fixMsgCreate.executionReportTrade(executionReportNew,
                                order.execBroker, order.lastPx);
                snowpipeStream.insert(executionReportTrade);
                // }

                snowpipeStream.close();
                logger.info("exit!!!!!");
        }
}
