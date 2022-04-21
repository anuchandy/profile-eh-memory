package com.eh.memory.publisher;

import com.microsoft.azure.eventhubs.*;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Publishes using track1 eventhubs library and notify the publish-completion using CompletableFuture.
 */
final class PublisherT1Impl implements PublisherInterface {
    private final EventHubClient client;
    private final ScheduledExecutorService scheduler;

    PublisherT1Impl() {
        this.scheduler = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors() * 4);

        final ConnectionStringBuilder connBuilder
                = new ConnectionStringBuilder(System.getenv("EH_CON_STR"))
                .setEventHubName(System.getenv("EH_NAME"));

        try {
            this.client = EventHubClient.createFromConnectionStringSync(
                    connBuilder.toString(),
                    scheduler);
        } catch (EventHubException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<Void> publish(byte[] data) {
        final EventDataBatch batch;
        try {
            batch = client.createBatch();
        } catch (EventHubException e) {
            throw new RuntimeException("Unable to create EventDataBatch.", e);
        }
        try {
            batch.tryAdd(EventData.create(data));
        } catch (PayloadSizeExceededException e) {
            throw new RuntimeException("Unable to add to EventDataBatch.", e);
        }
        return client.send(batch);
    }

    @Override
    public String getName() {
        return "T1";
    }

    @Override
    public void close() throws Exception {
        this.client.close();
        this.scheduler.shutdownNow();
    }
}