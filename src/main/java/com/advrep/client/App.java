package com.advrep.client;

import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App
{
    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    public static void main( String[] args )
    {
        AdvRepClient client = new AdvRepClient(7500, 12300, 4);

        LOG.info("Starting test");
        client.connectEdge("192.168.3.10");
        client.connectHub("192.168.3.11");
        performTest(client, 3);
        client.close();
        LOG.info("Test completed");
    }

    private static void performTest(AdvRepClient client, int times)
    {
        IntStream.rangeClosed(1, times).forEach((i) ->
        {
            System.out.println("Running time " + i + " out of " + times);

            client.setupEdge();
            client.setupHub();
            client.run();
        });
    }

}
