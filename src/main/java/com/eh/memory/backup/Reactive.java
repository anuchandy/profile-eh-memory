package com.eh.memory.backup;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerAsyncClient;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class Reactive {
    private final int ratePerSecond;
    private final int durationSeconds;
    private int recordNumber;
    private final long startTime = System.currentTimeMillis();
    private final EventHubProducerAsyncClient producerAsyncClient;

    public static void main( String[] args ) {
        Reactive fireAndForgetV1 = new Reactive(700, 60);
        fireAndForgetV1.run();
    }

    public Reactive(int ratePerSecond, int durationSeconds) {
        this.ratePerSecond = ratePerSecond;
        this.durationSeconds = durationSeconds;
        this.recordNumber = 0;
        this.producerAsyncClient = new EventHubClientBuilder()
                .connectionString(System.getenv("EH_CON_STR"), System.getenv("EH_NAME"))
                .buildAsyncProducerClient();
    }

    public void run(){
        System.out.println("Will run for "+durationSeconds+"s, publishing at a rate of "+ratePerSecond+" messages per second");
        ScheduledExecutorService loggerScheduler = Executors.newScheduledThreadPool(1);
        loggerScheduler.scheduleAtFixedRate(this::measureMemoryUsed, 1, 1, TimeUnit.SECONDS);
        long millisBetween = 1000 / ratePerSecond;    // roughly. Doesn't matter too much.
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        scheduler.scheduleAtFixedRate(()->{
            long elapsedMillis = System.currentTimeMillis() - startTime;
            long shouldHavePublished = (elapsedMillis * ratePerSecond)/1000;
            long shortfall = shouldHavePublished - recordNumber;

            for (int i=0; i<shortfall; i++) {
                Mono<EventDataBatch> batch = producerAsyncClient.createBatch();
                batch.flatMap(b -> {
                    b.tryAdd(new EventData(("aSmallMessage" + recordNumber).getBytes(StandardCharsets.UTF_8)));
                    return producerAsyncClient.send(b);
                }).subscribe();                 // Using .block() here cures the memory problem. But is slow – not async.
                recordNumber++;
            }

        }, 0, millisBetween, TimeUnit.MILLISECONDS);

        try {
            scheduler.awaitTermination(this.durationSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        loggerScheduler.shutdown();
    }

    private void measureMemoryUsed(){
        long kUsed = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())/1024;
        long megUsed = kUsed/1024;
        long elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000;
        System.out.println(elapsedSeconds+"s: After "+recordNumber+" records, memory used = "+megUsed+"MB");
    }

}
