package com.eh.memory.publisher;

/**
 * Factory to create eventhubs publishers.
 */
public class PublisherFactory {
    /**
     * @return a publisher backed by track1 completable-future based eventhubs library.
     */
    public static PublisherInterface createT1() {
        return new PublisherT1Impl();
    }

    /**
     *  @return a publisher backed by track1 reactor based eventhubs library.
     */
    public static PublisherInterface createT2() {
        return new PublisherT2Impl();
    }
}
