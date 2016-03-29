package com.advrep.client;

import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class AdvRepClient
{
    private static final String KEYSPACE = "demo";
    private static final String FAST_TABLE = "sensor_readings";
    private static final String SLOW_TABLE = "sensor_readings_slow";

    private static final String ADVREP_KEYSPACE = "dse_advrep";
    private static final String REPLICATION_LOG_TABLE = "advrep_replication_log";

    private static final int SENSORS = 100;

    private final int fastMessages;
    private final int slowMessages;

    private Cluster edgeCluster;
    private Cluster hubCluster;

    private Session sessionEdge;
    private Session sessionHub;

    private final MetricRegistry metrics = new MetricRegistry();
    private final int threads;

    private Timer fastTimer;
    private Timer slowTimer;

    private final ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();

    public AdvRepClient(int fastMessages, int slowMessages, int threads) {
        this.fastMessages = fastMessages;
        this.slowMessages = slowMessages;
        this.threads = threads;
    }

    public void run() {
        // Use new timers in every run
        metrics.remove("fast");
        metrics.remove("slow");

        fastTimer = metrics.timer("fast");
        slowTimer = metrics.timer("slow");

        ExecutorService executor = Executors.newFixedThreadPool(threads);

        long start = System.currentTimeMillis();

        List<Future<?>> futures = spawnWorkers(executor, threads);

        try
        {
            futures.forEach((f) -> {
                try
                {
                    f.get();
                }
                catch (InterruptedException | ExecutionException e)
                {
                    e.printStackTrace();
                }
            });
            long end = System.currentTimeMillis();
            reporter.report();
            System.out.println("Time elapsed: " + (end - start) + " milliseconds");
        }
        finally
        {
            executor.shutdown();
        }

    }

    private List<Future<?>> spawnWorkers(ExecutorService executor, int threads)
    {
        int totalMessages = fastMessages + slowMessages;
        float messagesPerThread = (float)totalMessages / threads;

        int fastTrackWorkers = (int)(fastMessages / messagesPerThread);
        int slowTrackWorkers = threads - fastTrackWorkers;

        System.out.println("Spawning " + fastTrackWorkers + " threads for fast track and " + slowTrackWorkers + " threads for slow track");

        int fastMessagesPerWorker = fastMessages / fastTrackWorkers;
        int slowMessagesPerWorker = slowMessages / slowTrackWorkers;

        List<Future<?>> fastFutures = IntStream.rangeClosed(1, fastTrackWorkers)
                .mapToObj((i) -> executor.submit(new Worker(true, calculateMessages(fastMessages, fastTrackWorkers, fastMessagesPerWorker, i))))
                .collect(Collectors.toList());

        List<Future<?>> slowFutures = IntStream.rangeClosed(1, slowTrackWorkers)
                .mapToObj((i) -> executor.submit(new Worker(false, calculateMessages(slowMessages, slowTrackWorkers, slowMessagesPerWorker, i))))
                .collect(Collectors.toList());

        fastFutures.addAll(slowFutures);
        return fastFutures;
    }

    private int calculateMessages(int totalMessages, int workers, int messagesPerWorker, int workerIndex)
    {
        return workerIndex == workers ?
                (totalMessages - (messagesPerWorker * (workerIndex - 1))) :
                messagesPerWorker;
    }

    private void insertReading(String table, int sensor, int reading, boolean isFast)
    {
        try
        {
            Timer.Context context = isFast ? fastTimer.time() : slowTimer.time();

            Statement statement = QueryBuilder.insertInto(KEYSPACE, table)
                    .value("sensor_id", String.valueOf(sensor))
                    .value("time", new Date())
                    .value("reading", reading);

            sessionEdge.execute(statement);

            context.stop();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    private int getSensor(int counter) {
        return counter % SENSORS;
    }

    private int generateReading() {
        Random r = new Random();
        return r.nextInt(100_000);
    }

    public void connectEdge(String edgeNode) {
        edgeCluster = Cluster.builder()
                .addContactPoint(edgeNode)
                .build();

        Metadata edgeMetadata = edgeCluster.getMetadata();

        System.out.printf("Connected to edge cluster: %s\n", edgeMetadata.getClusterName());

        for (Host host : edgeMetadata.getAllHosts() ) {
            System.out.printf("Edge datacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(),
                    host.getRack());
        }

        sessionEdge = edgeCluster.connect();
    }

    public void connectHub(String hubNode) {
        hubCluster = Cluster.builder()
                .addContactPoint(hubNode)
                .withCredentials("max", "max")
                .build();

        Metadata hubMetadata = hubCluster.getMetadata();

        System.out.printf("Connected to hub cluster: %s\n", hubMetadata.getClusterName());

        for (Host host : hubMetadata.getAllHosts() ) {
            System.out.printf("Hub datacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(),
                    host.getRack());
        }

        sessionHub = hubCluster.connect();
    }

    public void setupEdge() {
        sessionEdge.execute(QueryBuilder.truncate(ADVREP_KEYSPACE, REPLICATION_LOG_TABLE));
        sessionEdge.execute(QueryBuilder.truncate(KEYSPACE, FAST_TABLE));
        sessionEdge.execute(QueryBuilder.truncate(KEYSPACE, SLOW_TABLE));
    }

    public void setupHub() {
        sessionHub.execute(QueryBuilder.truncate(KEYSPACE, FAST_TABLE));
        sessionHub.execute(QueryBuilder.truncate(KEYSPACE, SLOW_TABLE));
    }

    public void close() {
        if (sessionEdge != null)
            sessionEdge.close();

        if (edgeCluster != null)
            edgeCluster.close();

        if (sessionHub != null)
            sessionHub.close();

        if (hubCluster != null)
            hubCluster.close();
    }

    private class Worker implements Runnable
    {
        private final int messagesNumber;
        private final String table;
        private final boolean isFast;

        private Worker(boolean isFast, int messagesNumber)
        {
            this.isFast = isFast;
            this.messagesNumber = messagesNumber;
            this.table = isFast ? FAST_TABLE : SLOW_TABLE;
        }

        @Override
        public void run()
        {
            IntStream.rangeClosed(1, messagesNumber).forEach(i -> {
                insertReading(table, getSensor(i), generateReading(), isFast);
            });
        }
    }

}
