package com.eh.memory.publisher;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerAsyncClient;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;

/**
 * Publishes using track2 eventhubs library and notify the publish-completion using CompletableFuture.
 */
final class PublisherT2Impl implements PublisherInterface {
    private final EventHubProducerAsyncClient producer;

    PublisherT2Impl() {
        this.producer = new EventHubClientBuilder()
                .connectionString(System.getenv("EH_CON_STR"), System.getenv("EH_NAME"))
                .buildAsyncProducerClient();
    }

    @Override
    public CompletableFuture<Void> publish(byte[] data) {
        Mono<Void> mono = producer.createBatch().flatMap(batch -> {
            batch.tryAdd(new EventData(data));
            return producer.send(batch);
        });
        return mono.toFuture();
    }

    @Override
    public String getName() {
        return "T2";
    }

    @Override
    public void close() throws Exception {
        this.producer.close();
    }
}