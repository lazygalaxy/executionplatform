package com.arcticarchitects.executionplatform;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.arcticarchitects.executionplatform.blocks.Customer;
import com.arcticarchitects.executionplatform.blocks.Order;

import quickfix.field.OrdType;
import quickfix.field.Side;
import quickfix.fix42.ExecutionReport;

public class App {
        private static final Logger logger = LogManager.getLogger(FixMsgCreate.class);
        private static final Random random = new Random();

        private static final List<Customer> normalCustomer = new ArrayList<Customer>();
        private static final List<Customer> zetaHeavyCustomer = new ArrayList<Customer>();
        private static final List<Asset> assets = new ArrayList<Asset>();
        private static final List<Order> orders = new ArrayList<Order>();

        private static FixMsgCreate fixMsgCreate;
        private static SnowpipeStream snowpipeStream;

        public static void main(String[] args) throws Exception {
                fixMsgCreate = new FixMsgCreate();
                snowpipeStream = new SnowpipeStream();

                normalCustomer.add(new Customer("BLOOMBERG", "APLHA BANK"));
                normalCustomer.add(new Customer("BLOOMBERG", "BETA BANK"));
                normalCustomer.add(new Customer("BLOOMBERG", "GAMMA BANK"));
                normalCustomer.add(new Customer("NYFIX", "DELTA BANK"));
                normalCustomer.add(new Customer("NYFIX", "EPSILON BANK"));
                normalCustomer.add(new Customer("AUTEX", "ZETA BANK"));

                zetaHeavyCustomer.add(new Customer("BLOOMBERG", "APLHA BANK"));
                zetaHeavyCustomer.add(new Customer("AUTEX", "ZETA BANK"));
                zetaHeavyCustomer.add(new Customer("BLOOMBERG", "BETA BANK"));
                zetaHeavyCustomer.add(new Customer("AUTEX", "ZETA BANK"));
                zetaHeavyCustomer.add(new Customer("BLOOMBERG", "GAMMA BANK"));
                zetaHeavyCustomer.add(new Customer("AUTEX", "ZETA BANK"));
                zetaHeavyCustomer.add(new Customer("NYFIX", "DELTA BANK"));
                zetaHeavyCustomer.add(new Customer("AUTEX", "ZETA BANK"));
                zetaHeavyCustomer.add(new Customer("NYFIX", "EPSILON BANK"));
                zetaHeavyCustomer.add(new Customer("AUTEX", "ZETA BANK"));

                assets.add(new Asset("MSFT", "USD", "XNAS", 429.03, "BOFA"));
                assets.add(new Asset("NVDA", "USD", "XNAS", 147.07, "BOFA"));
                assets.add(new Asset("CRM", "USD", "XNAS", 332.62, "BOFA"));
                assets.add(new Asset("MCD", "USD", "XNAS", 281.35, "BOFA"));
                assets.add(new Asset("IBM", "USD", "XNAS", 223.26, "BOFA"));

                assets.add(new Asset("ABBN", "CHF", "XSWX", 53.44, "DMA"));
                assets.add(new Asset("ALC", "CHF", "XSWX", 80.68, "DMA"));
                assets.add(new Asset("GEBN", "CHF", "XSWX", 502.60, "DMA"));
                assets.add(new Asset("GIVN", "CHF", "XSWX", 3947.00, "DMA"));
                assets.add(new Asset("HOLN", "CHF", "XSWX", 89.12, "DMA"));
                assets.add(new Asset("KNIN", "CHF", "XSWX", 207.20, "DMA"));
                assets.add(new Asset("LOGN", "CHF", "XSWX", 82.58, "DMA"));
                assets.add(new Asset("LONN", "CHF", "XSWX", 581.80, "DMA"));
                assets.add(new Asset("NESN", "CHF", "XSWX", 74.06, "DMA"));
                assets.add(new Asset("NOVN", "CHF", "XSWX", 80.11, "DMA"));
                assets.add(new Asset("PGHN", "CHF", "XSWX", 1379.00, "DMA"));
                assets.add(new Asset("UBS", "CHF", "XSWX", 31.40, "DMA"));
                assets.add(new Asset("ROG", "CHF", "XSWX", 274.60, "DMA"));
                assets.add(new Asset("SIKA", "CHF", "XSWX", 227.40, "DMA"));
                assets.add(new Asset("SOON", "CHF", "XSWX", 316.00, "DMA"));

                assets.add(new Asset("LVHM", "EUR", "XPAR", 771.70, "DMA"));
                assets.add(new Asset("RMS", "EUR", "XPAR", 2720.00, "DMA"));
                assets.add(new Asset("LOREAL", "EUR", "XPAR", 369.90, "DMA"));
                assets.add(new Asset("SCHNEI", "EUR", "XPAR", 281.60, "DMA"));
                assets.add(new Asset("AIRBUS", "EUR", "XPAR", 171.50, "DMA"));

                orders.add(new Order(Side.BUY, OrdType.LIMIT));
                orders.add(new Order(Side.SELL, OrdType.MARKET));

                // historicScenarion(LocalDateTime.of(2025, 1, 2, 14, 30, 0));

                // Normal
                realTimeScenarion(13, 1000, normalCustomer);

                // Latency Increase
                // realTimeScenarion(100,1000, normalCustomer);

                // Order Burst
                // realTimeScenarion(13,250, normalCustomer);

                // Zeta Bank Burst
                // realTimeScenarion(13,250, zetaHeavyCustomer);

                snowpipeStream.close();
                logger.info("exit!!!!!");
        }

        private static void realTimeScenarion(long latency, long sleep, List<Customer> customers) throws Exception {
                while (System.in.available() == 0) {
                        Thread.sleep(sleep);
                        LocalDateTime now = LocalDateTime.now();
                        insert(now, now.plus(latency + 7, ChronoUnit.MILLIS), latency, customers);
                }
        }

        private static void historicScenarion(LocalDateTime startDateTime) throws Exception {
                LocalDateTime currentDateTime = startDateTime.withHour(14).withMinute(30).withSecond(0);
                LocalDateTime nowDateTime = LocalDateTime.now();
                while (currentDateTime.isBefore(nowDateTime)) {
                        logger.info(currentDateTime);
                        insert(currentDateTime, currentDateTime.plus(20, ChronoUnit.MILLIS), 13, normalCustomer);
                        currentDateTime = currentDateTime.plusSeconds(1);
                        if (currentDateTime.getHour() >= 15 && currentDateTime.getMinute() > 30)
                                currentDateTime = currentDateTime.plusDays(1).withHour(14).withMinute(30).withSecond(0);

                }
        }

        private static void insert(LocalDateTime newTransactTime, LocalDateTime tradeTransactTime, long latency,
                        List<Customer> customers)
                        throws Exception {
                Customer customer = customers.get(random.nextInt(customers.size()));
                Asset asset = assets.get(random.nextInt(assets.size()));
                Order order = orders.get(random.nextInt(orders.size()));

                ExecutionReport executionReportNew = fixMsgCreate.executionReportNew(
                                order.side,
                                order.ordType,
                                random.nextInt(100) * 100,
                                asset.price,
                                asset.symbol,
                                asset.currency,
                                asset.securityExchange,
                                customer.senderCompID,
                                customer.account, newTransactTime, latency);
                snowpipeStream.insert(executionReportNew);

                ExecutionReport executionReportTrade = fixMsgCreate.executionReportTrade(executionReportNew,
                                asset.execBroker, asset.getLastPx(order.side), tradeTransactTime, latency);
                snowpipeStream.insert(executionReportTrade);
        }
}
