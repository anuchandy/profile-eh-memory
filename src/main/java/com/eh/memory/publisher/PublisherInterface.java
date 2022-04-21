package com.eh.memory.publisher;

import java.util.concurrent.CompletableFuture;

/**
 * A contract for publishing to eventhubs.
 */
public interface PublisherInterface extends AutoCloseable {
    /**
     * Publishes data to eventhubs and notify the publish-completion using CompletableFuture.
     *
     * @param data the data to publish
     * @return CompletableFuture object indicating the publish-completion
     */
    CompletableFuture<Void> publish(byte[] data);

    /**
     * human readble name for debugging.
     *
     * @return the name of the contract implementation
     */
    String getName();
}
