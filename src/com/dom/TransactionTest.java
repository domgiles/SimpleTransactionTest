package com.dom;

import oracle.jdbc.OracleDriver;
import oracle.jdbc.pool.OracleDataSource;
import oracle.security.pki.OracleWallet;
import oracle.security.pki.textui.OraclePKIGenFunc;
import oracle.ucp.jdbc.*;
import org.apache.commons.cli.*;

import javax.crypto.Cipher;
import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static java.util.logging.Level.FINE;

public class TransactionTest {

    private static final String[] AlphaDataArray = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "v", "w", "u", "x", "y", "z", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "V", "W", "U", "X", "Y", "Z", "1", "2", "3", "4", "5", "6", "7", "8", "9", "0"};


    static final String insCustomer = """
            insert into customers (customer_id,
                                   cust_first_name,
                                   cust_last_name,
                                   nls_language,
                                   nls_territory,
                                   credit_limit,
                                   cust_email,
                                   account_mgr_id,
                                   customer_since,
                                   customer_class,
                                   suggestions,
                                   dob,
                                   mailshot,
                                   partner_mailshot,
                                   preferred_address,
                                   preferred_card)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""";

    private static final Logger logger = Logger.getLogger(TransactionTest.class.getName());

    private enum CommandLineOptions {
        USERNAME, PASSWORD, CONNECT_STRING, THREAD_COUNT, CONNECTION_TYPE, DRIVER_TYPE, RUN_TIME, THINK_TIME, CONNECTION_POOL, USE_FAN, USE_AC_DRIVER, POOL_SIZE, SHOW_STATS
    }

    private enum ResultsMetric {
        CONNECTION_TIME, TOTAL_TRANSACTION_TIME, TOTAL_TRANSACTIONS_COMPLETED, FAILED_TRANSACTIONS, AVERAGE_RESPONSE_TIME
    }

    private enum ConnectionType {
        PDS, ODS
    }

    private enum DriverType {
        oci, thin
    }

    static boolean benchmarkRunning = true;
    static final Random rand = new Random(System.nanoTime());
    static final Map<Integer, String> exceptionsList = new HashMap<>();
    static final String DEFAULT_TIME_MASK = "%1$tH:%1$tM:%1$tS";
    static boolean errorsOccurred = false;
    static Long totalTransactionsCompleted = 0L;


    private static void processThreadExceptions(SQLException e) {
        int code = e.getErrorCode();
        String message = e.getMessage();
        errorsOccurred = true;
        exceptionsList.put(code, message);
    }

    private static Map<ResultsMetric, Object> runTransactionWorkLoad(Map<CommandLineOptions, Object> pclo) throws RuntimeException, Error {
        Map<ResultsMetric, Object> results = new HashMap<>();

        try {
            String username = (String) pclo.get(CommandLineOptions.USERNAME);
            String password = (String) pclo.get(CommandLineOptions.PASSWORD);
            String connectString = String.format("jdbc:oracle:%s:@%s", pclo.get(CommandLineOptions.DRIVER_TYPE).toString(), pclo.get(CommandLineOptions.CONNECT_STRING));
            Long thinkTime = (Long) pclo.get(CommandLineOptions.THINK_TIME);

            long timer1 = 0;
            Connection connection = null;
            timer1 = System.currentTimeMillis();
            if (pclo.get(CommandLineOptions.CONNECTION_TYPE) == ConnectionType.ODS) {
                OracleDataSource ods = new OracleDataSource();
                ods.setUser(username);
                ods.setPassword(password);
                ods.setURL(connectString);
                Properties connectionProperties = new Properties();
                connectionProperties.setProperty("autoCommit", "false");
                connectionProperties.setProperty("oracle.jdbc.fanEnabled", "false");
                ods.setConnectionProperties(connectionProperties);

                connection = ods.getConnection();
            }
            results.put(ResultsMetric.CONNECTION_TIME, System.currentTimeMillis() - timer1);
            long timer2 = System.currentTimeMillis();
            long transactionCount = 0;
            long totalResponseTime = 0;

            while (benchmarkRunning) {

                long timer3 = System.nanoTime();
                if (pclo.get(CommandLineOptions.CONNECTION_TYPE) == ConnectionType.PDS) {
                    PoolDataSource pds = (PoolDataSource) pclo.get(CommandLineOptions.CONNECTION_POOL);
                    connection = pds.getConnection();
                }
                try (PreparedStatement custPs = connection.prepareStatement(insCustomer);
                     PreparedStatement seqPs = connection.prepareStatement("select customer_seq.nextval from dual")) {
                    // Insert a row into database and then retrieve it
                    // First get a sequence
                    try (ResultSet rs = seqPs.executeQuery()) {
                        rs.next();
                        long custID = rs.getLong(1);
                        Date dob = new Date(System.currentTimeMillis() - (randomLong(18, 65) * 31556952000L));
                        Date custSince = new Date(System.currentTimeMillis() - (randomLong(1, 4) * 31556952000L));
                        String firstName = randomAlpha(6, 12);
                        String lastName = randomAlpha(6, 12);
                        custPs.setLong(1, custID);
                        custPs.setString(2, firstName);
                        custPs.setString(3, lastName);
                        custPs.setString(4, "EN");
                        custPs.setString(5, "GB");
                        custPs.setInt(6, randomInteger(100, 1000));
                        custPs.setString(7, firstName + "." + lastName + "@" + "oracle.com");
                        custPs.setInt(8, randomInteger(145, 179));
                        custPs.setDate(9, custSince);
                        custPs.setString(10, "Ocasional");
                        custPs.setString(11, "Music");
                        custPs.setDate(12, dob);
                        custPs.setString(13, "Y");
                        custPs.setString(14, "N");
                        custPs.setLong(15, -1);
                        custPs.setLong(16, custID);
                        custPs.execute();
                        // Commit the row
                        connection.commit();
                        // Retrieve the row that's just been inserted
                        try (PreparedStatement custDetPs = connection.prepareStatement("""
                                select customer_id, cust_first_name, cust_last_name, nls_language,\s
                                  nls_territory, credit_limit, cust_email, account_mgr_id, customer_since,\s
                                  customer_class, suggestions, dob, mailshot, partner_mailshot,\s
                                  preferred_address, preferred_card\s
                                from
                                 customers where customer_id = ?""")) {
                            custDetPs.setLong(1, custID);
                            try (ResultSet crs = custDetPs.executeQuery()) {
                                if (crs.next()) {
                                    int id = crs.getInt(1);
                                    String name = crs.getString(2);
                                }
                            }
                        }
                        transactionCount += 1;
                        totalTransactionsCompleted += 1;
                        // Close the connection pool if needed
                        if (pclo.get(CommandLineOptions.CONNECTION_TYPE) == ConnectionType.PDS) {
                            connection.close();
                            connection = null;
                        }
                        totalResponseTime = System.nanoTime() - timer3;
                        // Sleep for a random period of time
                        try {
                            Thread.sleep(randomLong(thinkTime / 2, thinkTime));
                        } catch (InterruptedException ignore) {
                        }
                    }
                }
            }
            results.put(ResultsMetric.TOTAL_TRANSACTIONS_COMPLETED, transactionCount);
            results.put(ResultsMetric.TOTAL_TRANSACTION_TIME, System.currentTimeMillis() - timer2);
            results.put(ResultsMetric.AVERAGE_RESPONSE_TIME, transactionCount == 0 ? 0d : (((double) totalResponseTime / transactionCount)) / 1000000d);
//            System.out.printf("Total Transactions = %d%n", transactionCount);
            return results;
        } catch (SQLException e) {
            logger.log(FINE, e.getMessage());
            processThreadExceptions(e);
            return results;
        }

    }

    public static Map<ResultsMetric, Object> runTransactions(Map<CommandLineOptions, Object> pclo) {
        return runTransactionWorkLoad(pclo);
    }

    private static List<Map<ResultsMetric, Object>> connectBenchmark(Map<CommandLineOptions, Object> pclo) throws Exception {
        List<Map<ResultsMetric, Object>> connectResults;

        // If using connection pool, create pool

        if (pclo.get(CommandLineOptions.CONNECTION_TYPE) == ConnectionType.PDS) {
            try {
                Properties connectionProperties = new Properties();
                String username = (String) pclo.get(CommandLineOptions.USERNAME);
                String password = (String) pclo.get(CommandLineOptions.PASSWORD);
                String connectString = String.format("jdbc:oracle:%s:@%s", pclo.get(CommandLineOptions.DRIVER_TYPE).toString(), pclo.get(CommandLineOptions.CONNECT_STRING));
                logger.log(FINE, String.format("Connecting to %s with username \"%s\" with password \"%s\"", connectString, username, password));
                PoolDataSource pds = PoolDataSourceFactory.getPoolDataSource();
//                PoolDataSource pds = PoolDataSourceFactory.getPoolDataSource();
                if ((boolean) pclo.get(CommandLineOptions.USE_AC_DRIVER)) {
                    pds.setConnectionFactoryClassName("oracle.jdbc.replay.OracleDataSourceImpl");
                    connectionProperties.setProperty("oracle.jdbc.fanEnabled", "true");
                } else {
                    pds.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
                    connectionProperties.setProperty("oracle.jdbc.fanEnabled", "false");
                }
                System.out.printf("%sEstablishing a connection pool using %s%s%s driver of size %s%d%s, Connection String is %s%s%s, Using application continuity driver is %s%b%s%s%n",
                        ConsoleColours.BLUE,
                        ConsoleColours.BLUE_BOLD_BRIGHT, pclo.get(CommandLineOptions.DRIVER_TYPE), ConsoleColours.BLUE,
                        ConsoleColours.BLUE_BOLD_BRIGHT, (Integer) pclo.get(CommandLineOptions.POOL_SIZE), ConsoleColours.BLUE,
                        ConsoleColours.BLUE_BOLD_BRIGHT, connectString, ConsoleColours.BLUE,
                        ConsoleColours.BLUE_BOLD_BRIGHT, pclo.get(CommandLineOptions.USE_AC_DRIVER), ConsoleColours.BLUE,
                        ConsoleColours.RESET);
                pds.setURL(connectString);
                pds.setUser(username);
                pds.setPassword(password);
                pds.setConnectionPoolName("CONN_TEST_POOL");
                pds.setMinPoolSize(1);
                pds.setInitialPoolSize((Integer) pclo.get(CommandLineOptions.POOL_SIZE));
                pds.setMinPoolSize((Integer) pclo.get(CommandLineOptions.POOL_SIZE)/2);
                pds.setMaxPoolSize((Integer) pclo.get(CommandLineOptions.POOL_SIZE));
                pds.setTimeoutCheckInterval(2);
                pds.setInactiveConnectionTimeout(10);
                pds.setConnectionWaitTimeout(20);
                pds.setAbandonedConnectionTimeout(20);
                connectionProperties.setProperty("autoCommit", "false");
                connectionProperties.setProperty("oracle.jdbc.fanEnabled", "false");
                pds.setConnectionProperties(connectionProperties);
                pds.setConnectionPoolName("TransactionTest");
                // Check if it's possible to get a connection
                Connection connection = pds.getConnection();
                connection.close();
                pclo.put(CommandLineOptions.CONNECTION_POOL, pds);
            } catch (SQLException e) {
                logger.log(FINE, String.format("%s", e.getMessage()));
                System.err.printf("%sUnable to establish a connection pool to %s, See the following message : %s%s", ConsoleColours.RED_BOLD, pclo.get(CommandLineOptions.CONNECT_STRING), ConsoleColours.RESET, e.getMessage());
                System.exit(-1);
            }
        }
        logger.fine("Started creating connections");
        List<Callable<Map<ResultsMetric, Object>>> connectTests = new ArrayList<>();
        logger.fine("Creating threads");
        for (int i = 0; i < (Integer) pclo.get(CommandLineOptions.THREAD_COUNT); i++) {
            Callable<Map<ResultsMetric, Object>> connectTask = () -> runTransactions(pclo);
            connectTests.add(connectTask);
        }
        logger.fine("Created threads");
        Timer timer = new Timer("BenchmarkTimer");
        TimerTask task = new TimerTask() {
            public void run() {
                benchmarkRunning = false;
                System.out.printf("%sTimer fired. Finishing Benchmark %s%n", ConsoleColours.BLUE, ConsoleColours.RESET);
            }
        };
        timer.schedule(task, (long) pclo.get(CommandLineOptions.RUN_TIME));
        ExecutorService executor = Executors.newWorkStealingPool((Integer) pclo.get(CommandLineOptions.THREAD_COUNT) + 1);
        logger.fine("Asking Threads to connect");
        PoolDataSource pds = (PoolDataSource) pclo.get(CommandLineOptions.CONNECTION_POOL);
        Runnable runnable = () -> {
            String header[] = null;
            if (pclo.get(CommandLineOptions.CONNECTION_TYPE) == ConnectionType.PDS) {
                header = new String[]{"Time", "TPS", "Available", "Borrowed", "Created", "Closed", "Wait Time"};
//                header = new String[]{"Time", "TPS", "Available", "Borrowed", "Returned", "Created", "Closed", "Wait Time"};
            } else {
                header = new String[]{"Time", "TPS"};
            }
            int minSpacing = 10;
            String hf = formatStringArray(header, minSpacing, ConsoleColours.YELLOW);
            System.out.println(hf);
            long oldTotalTransactionsCompleted = 0;
            while (benchmarkRunning) {
                String[] values;
                long txDifference = totalTransactionsCompleted - oldTotalTransactionsCompleted;
                oldTotalTransactionsCompleted = totalTransactionsCompleted;
                if (pclo.get(CommandLineOptions.CONNECTION_TYPE) == ConnectionType.PDS) {
                    values = new String[]{String.format(DEFAULT_TIME_MASK, new java.util.Date(System.currentTimeMillis())),
                            Long.toString(txDifference),
                            Integer.toString(pds.getStatistics().getAvailableConnectionsCount()),
                            Integer.toString(pds.getStatistics().getBorrowedConnectionsCount()),
//                            Long.toString(pds.getStatistics().getCumulativeConnectionReturnedCount()),
                            Integer.toString(pds.getStatistics().getConnectionsCreatedCount()),
                            Integer.toString(pds.getStatistics().getConnectionsClosedCount()),
                            Long.toString(pds.getStatistics().getAverageConnectionWaitTime())
                    };
                } else {
                    values = new String[]{String.format(DEFAULT_TIME_MASK, new java.util.Date(System.currentTimeMillis())),
                            Long.toString(txDifference)};
                }
                System.out.println(formatStringArray(values, minSpacing, ConsoleColours.CYAN_BRIGHT));
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignore) {
                }
            }
        };
        executor.submit(runnable);
        connectResults = executor.invokeAll(connectTests).stream().map(future -> {
            try {
                return future.get();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }).collect(Collectors.toList());

        return connectResults;
    }

    private static String formatStringArray(String[] header, int minSpacing, String colour) {
        String[] result1 = new String[header.length];
        for (int i = 0; i < header.length; i++) {
            int l = Math.max(header[i].length(), minSpacing);
            result1[i] = (String.format("%s%s %s", colour, String.format("%%%ds", l), ConsoleColours.RESET));
        }
        StringBuilder result2 = new StringBuilder();
        for (int i = 0; i < result1.length; i++) {
            result2.append(String.format(result1[i], header[i]));
        }
        return result2.toString();
    }

    public static void main(String[] args) {
        Map<CommandLineOptions, Object> pclo = parseCommandLine(args);
        try {
            logger.fine("Starting...");

            Properties properties = System.getProperties();
            properties.entrySet().stream().filter(k -> (k.getKey().toString().startsWith("oracle.net") || k.getKey().toString().startsWith("javax.net"))).forEach(k -> System.out.printf("%35s -> %s%n", k.getKey(), k.getValue()));

            long startMillis = System.currentTimeMillis();
            System.out.printf("%sStarting Simple Transaction Test%s%n", ConsoleColours.BLUE, ConsoleColours.RESET);
            System.out.printf("%sUsing Oracle Driver version %s%s%s, Built on %s%s%n", ConsoleColours.BLUE, ConsoleColours.BLUE_BOLD_BRIGHT, OracleDriver.getDriverVersion(), ConsoleColours.BLUE, OracleDriver.getBuildDate(), ConsoleColours.RESET);
            System.out.printf("%sConnecting using a %s%s%s driver%s%n", ConsoleColours.BLUE, ConsoleColours.BLUE_BOLD_BRIGHT, pclo.get(CommandLineOptions.DRIVER_TYPE), ConsoleColours.BLUE, ConsoleColours.RESET);
            List<Map<ResultsMetric, Object>> connectResults = connectBenchmark(pclo);
            if (!errorsOccurred) {
                OptionalDouble avgConnectTime = connectResults.stream().mapToLong(r -> (Long) r.get(ResultsMetric.CONNECTION_TIME)).average();
//                OptionalDouble avgRunTime = connectResults.stream().mapToLong(r -> (Long) r.get(ResultsMetric.TOTAL_TRANSACTION_TIME)).average();
                OptionalDouble avgResponseTime = connectResults.stream().mapToDouble(r -> (Double) r.get(ResultsMetric.AVERAGE_RESPONSE_TIME)).average();
                Long totalTransactions = connectResults.stream().mapToLong(r -> (Long) r.get(ResultsMetric.TOTAL_TRANSACTIONS_COMPLETED)).sum();

                System.out.printf("%sConnected %s%d%s threads, Average connect time = %s%.2fms%s, Average response time = %s%.3fms%s, Total transactions completed = %s%d%s%n",
                        ConsoleColours.CYAN, ConsoleColours.RED, connectResults.size(),
                        ConsoleColours.CYAN, ConsoleColours.RED, avgConnectTime.orElse(0),
                        ConsoleColours.CYAN, ConsoleColours.RED, avgResponseTime.orElse(0), ConsoleColours.CYAN,
                        ConsoleColours.RED, totalTransactions, ConsoleColours.RESET);
                logger.fine("Finished...");
                System.exit(0);
            } else {
                System.err.printf("%sErrors occured during simple transaction workload.\n%s", ConsoleColours.RED, ConsoleColours.RESET);
                for (Map.Entry<Integer, String> kv : exceptionsList.entrySet()) {
                    System.err.printf("%s%d -> %s%s%s%n", ConsoleColours.RED, kv.getKey(), ConsoleColours.RED_BOLD, kv.getValue(), ConsoleColours.RESET);
                }
                System.exit(-1);
            }
        } catch (Exception e) {
            System.err.printf("%sUnable to run simple transaction workload test, See the following message :\n%s%s\n", ConsoleColours.RED_BOLD, ConsoleColours.RESET, e.getMessage());

            logger.log(Level.FINE, "Unexpected Exception thrown and not handled : ", e);
            System.exit(-1);
        }
    }

    private static Map<CommandLineOptions, Object> parseCommandLine(String[] arguments) {

        Map<CommandLineOptions, Object> parsedOptions = new HashMap<>();

        Options options = new Options();
        Option option8 = new Option("u", "username");
        option8.setRequired(true);
        option8.setArgName("username");
        option8.setArgs(1);
        Option option9 = new Option("p", "password");
        option9.setArgs(1);
        option9.setRequired(true);
        option9.setArgName("password");
        Option option10 = new Option("cs", "connect string");
        option10.setArgs(1);
        option10.setRequired(true);
        option10.setArgName("connectstring");
        Option option13 = new Option("ct", "pds or ods");
        option13.setArgs(1);
        option13.setArgName("threadcount");
        Option option14 = new Option("tc", "thread count, defaults to 1");
        option14.setArgs(1);
        option14.setArgName("threadcount");
        Option option25 = new Option("o", "output : valid values are stdout,csv");
        option25.setArgs(1);
        option14.setArgName("output");
        Option option26 = new Option("cf", "credentials file in zip format");
        option26.setArgs(1);
        option26.setArgName("zipfile");
        Option option27 = new Option("dt", "Driver Type [thin,oci]");
        option27.setArgs(1);
        option27.setArgName("driver_type");
        Option option28 = new Option("rt", "runtime of test");
        option28.setArgs(1);
        option28.setArgName("runtime");
        Option option29 = new Option("tt", "Think Time (milliseconds)");
        option29.setArgs(1);
        option29.setArgName("thinktime");
        Option option30 = new Option("ac", "use application continuity");
        option30.setArgs(0);
        Option option31 = new Option("ps", "Pool Size");
        option31.setArgs(1);
        option31.setArgName("pool size");

        Option option100 = new Option("debug", "turn on debugging. Written to standard out");

        options.addOption(option8).addOption(option9).addOption(option10).addOption(option30).addOption(option14).addOption(option25).addOption(option13).addOption(option26).addOption(option27).addOption(option28).addOption(option29).addOption(option30).addOption(option31).addOption(option100);
        CommandLineParser clp = new DefaultParser();
        CommandLine cl;
        try {
            cl = clp.parse(options, arguments);
            if (cl.hasOption("debug")) {
                try {
                    System.setProperty("java.util.logging.config.class", "com.dom.LoggerConfig");
                    LogManager.getLogManager().readConfiguration();
                } catch (IOException e) {
                    System.err.printf("%sUnable to find debug config files. Exiting%s%n", ConsoleColours.RED_BOLD, ConsoleColours.RESET);
                }
            }
            if (cl.hasOption("h")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("parameters:", options);
                System.exit(0);
            }
            if (cl.hasOption("u")) {
                parsedOptions.put(CommandLineOptions.USERNAME, cl.getOptionValue("u"));
            }
            if (cl.hasOption("p")) {
                parsedOptions.put(CommandLineOptions.PASSWORD, cl.getOptionValue("p"));
            }
            if (cl.hasOption("cs")) {
                parsedOptions.put(CommandLineOptions.CONNECT_STRING, cl.getOptionValue("cs"));
            }
            if (cl.hasOption("tc")) {
                parsedOptions.put(CommandLineOptions.THREAD_COUNT, Integer.parseInt(cl.getOptionValue("tc")));
            } else {
                parsedOptions.put(CommandLineOptions.THREAD_COUNT, 1);
            }
            if (cl.hasOption("rt")) {
                parsedOptions.put(CommandLineOptions.RUN_TIME, parseRunTime(cl.getOptionValue("rt")));
            } else {
                parsedOptions.put(CommandLineOptions.RUN_TIME, 1000L);
            }
            if (cl.hasOption("tt")) {
                parsedOptions.put(CommandLineOptions.THINK_TIME, Long.parseLong(cl.getOptionValue("tt")));
            } else {
                parsedOptions.put(CommandLineOptions.THINK_TIME, 0L);
            }


            parsedOptions.put(CommandLineOptions.CONNECTION_TYPE, ConnectionType.ODS);
            if (cl.hasOption("ct")) {
                if (cl.getOptionValue("ct").equals("pds")) {
                    parsedOptions.put(CommandLineOptions.CONNECTION_TYPE, ConnectionType.PDS);
                }
            }
            if (cl.hasOption("cf")) {
                if ((new File(cl.getOptionValue("cf")).exists())) {
                    setupSecureOracleCloudProperties("DummyPassw0rd!", cl.getOptionValue("cf"), true);
                } else {
                    System.err.printf("The credentials file %s does not exists. Please specify a valid path and retry.\n", cl.getOptionValue("cf"));
                    System.exit(-1);
                }
            }
            parsedOptions.put(CommandLineOptions.DRIVER_TYPE, DriverType.thin);
            if (cl.hasOption("dt")) {
                try {
                    DriverType dt = DriverType.valueOf(cl.getOptionValue("dt"));
                    parsedOptions.put(CommandLineOptions.DRIVER_TYPE, dt);
                } catch (IllegalArgumentException e) {
                    throw new ParseException("Driver Type must be \"oci\" or \"thin\"");
                }
            }
            if (cl.hasOption("ac")) {
                parsedOptions.put(CommandLineOptions.USE_AC_DRIVER, true);
                parsedOptions.put(CommandLineOptions.USE_FAN, true);
            } else {
                parsedOptions.put(CommandLineOptions.USE_AC_DRIVER, false);
                parsedOptions.put(CommandLineOptions.USE_FAN, false);
            }
            if (cl.hasOption("ps")) {
                parsedOptions.put(CommandLineOptions.POOL_SIZE, Integer.parseInt(cl.getOptionValue("ps")));
            } else {
                parsedOptions.put(CommandLineOptions.POOL_SIZE, -1);
            }

        } catch (ParseException pe) {
            System.out.println("ERROR : " + pe.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("parameters:", options);
            System.exit(-1);
        }
        return parsedOptions;

    }

    public static Path setupSecureOracleCloudProperties(String passwd, String credentialsLocation, Boolean deleteOnExit) throws RuntimeException {
        try {
            if (!testJCE()) {
                throw new RuntimeException("Extended JCE support is not installed.");
            }
            Path tmp = Files.createTempDirectory("oracle_cloud_config");
            Path origfile = Paths.get(credentialsLocation);
            if (deleteOnExit) {
                Runtime.getRuntime().addShutdownHook(new Thread(() -> recursiveDelete(tmp)));
            }

            Path pzip = tmp.resolve("temp.zip");
            Files.copy(origfile, pzip);

            ZipFile zf = new ZipFile(pzip.toFile());
            Enumeration<? extends ZipEntry> entities = zf.entries();
            while (entities.hasMoreElements()) {
                ZipEntry entry = entities.nextElement();
                String name = entry.getName();
                Path p = tmp.resolve(name);
                Files.copy(zf.getInputStream(entry), p);
            }

            String pathToWallet = tmp.toFile().getAbsolutePath();

            System.setProperty("oracle.net.tns_admin", pathToWallet);
            System.setProperty("oracle.net.ssl_server_dn_match", "true");
            System.setProperty("oracle.net.ssl_version", "1.2");

            // open the CA's wallet
            OracleWallet caWallet = new OracleWallet();
            caWallet.open(pathToWallet, null);


            char[] keyAndTrustStorePasswd = OraclePKIGenFunc.getCreatePassword(passwd, false);

            // certs
            OracleWallet jksK = caWallet.migratePKCS12toJKS(keyAndTrustStorePasswd, OracleWallet.MIGRATE_KEY_ENTIRES_ONLY);
            // migrate (trusted) cert entries from p12 to different jks store
            OracleWallet jksT = caWallet.migratePKCS12toJKS(keyAndTrustStorePasswd, OracleWallet.MIGRATE_TRUSTED_ENTRIES_ONLY);
            String trustPath = pathToWallet + "/sqlclTrustStore.jks";
            String keyPath = pathToWallet + "/sqlclKeyStore.jks";

            jksT.saveAs(trustPath);
            jksK.saveAs(keyPath);


            System.setProperty("javax.net.ssl.trustStore", trustPath);
            System.setProperty("javax.net.ssl.trustStorePassword", passwd);
            System.setProperty("javax.net.ssl.keyStore", keyPath);
            System.setProperty("javax.net.ssl.keyStorePassword", passwd);
            java.security.Security.addProvider(new oracle.security.pki.OraclePKIProvider());
            return tmp;

        } catch (Exception e) {
            logger.fine(String.format("Unable to open and process the credentials file %s.", credentialsLocation));
            throw new RuntimeException(e);
        }
    }

    private static boolean testJCE() {
        int maxKeySize = 0;
        try {
            maxKeySize = Cipher.getMaxAllowedKeyLength("AES");

        } catch (NoSuchAlgorithmException ignore) {
        }
        return maxKeySize > 128;
    }

    public static void recursiveDelete(final Path path) {

        try {
            Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, @SuppressWarnings("unused") BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException e) throws IOException {
                    if (e == null) {
                        Files.delete(dir);
                        return FileVisitResult.CONTINUE;
                    }
                    // directory iteration failed
                    throw e;
                }
            });
            logger.fine(String.format("Deleted tmp directory : %s", path));
        } catch (IOException e) {
            throw new RuntimeException("Failed to delete " + path, e);
        }
    }

    public static long parseRunTime(String rt) throws NumberFormatException {
        if (rt != null) {
            StringTokenizer st = new StringTokenizer(rt, ":");
            long hours = Long.parseLong(st.nextToken());
            long minutes;
            String minString = st.nextToken();
            int secs = 0;
            if (minString.contains(".")) {
                int loc = minString.indexOf(".");
                String secString = minString.substring(loc + 1);
                secs = Integer.parseInt(secString);
                if (secs >= 60) throw new NumberFormatException("Seconds must be less than 60");
                minutes = Long.parseLong(minString.substring(0, loc));

            } else {
                minutes = Long.parseLong(minString);
            }
            if (minutes >= 60) throw new NumberFormatException("Minutes must be less than 60");
            hours = hours * 60 * 60 * 1000;
            minutes = ((minutes * 60) + secs) * 1000;
            return hours + minutes;
        } else {
            return 0;
        }
    }

    public static long randomLong(long s, long e) {
        if ((e - s) != 0) {
            return (Math.abs(rand.nextLong()) % (e - s)) + s;
        }
        return e;
    }

    public static int randomInteger(int s, int e) {
        if ((e - s) != 0) {
            return (Math.abs(rand.nextInt()) % (e - s)) + s;
        }
        return e;
    }

    public static String randomAlpha(int s, int e) {
        StringBuilder colvalue = new StringBuilder();

        int strlength = ((e - s) > 0) ? (Math.abs(rand.nextInt()) % (e - s)) + s : e;

        for (int i = 0; i < strlength; i++) {
            colvalue.append(AlphaDataArray[Math.abs(rand.nextInt() % (AlphaDataArray.length - 10))]);
        }

        return (colvalue.toString());
    }


}


