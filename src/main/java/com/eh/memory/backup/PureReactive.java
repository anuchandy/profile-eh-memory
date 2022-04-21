package com.eh.memory.backup;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerAsyncClient;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class PureReactive implements AutoCloseable {
    private final int ratePerSecond;
    private final ScheduledExecutorService loggerScheduler;
    private final AtomicInteger recordNumber = new AtomicInteger();
    private final long startTime = System.currentTimeMillis();
    private final EventHubProducerAsyncClient producerAsyncClient;

    public PureReactive(int ratePerSecond) {
        this.ratePerSecond = ratePerSecond;
        this.producerAsyncClient = new EventHubClientBuilder()
                .connectionString(System.getenv("EH_CON_STR"), System.getenv("EH_NAME"))
                .buildAsyncProducerClient();
        this.loggerScheduler = Executors.newScheduledThreadPool(1);
    }

    public static void main(String[] args) {
        System.out.println("Max memory = " + (Runtime.getRuntime().maxMemory() / (1024 * 1024 * 1024) + "G"));

        final int durationInSeconds = 60;

        // This runs fine. 100 messages per second ok, but using more memory. 700 messages per second leads to OOM within 30 seconds.
        final int ratePerSecond = 1500;
        final PureReactive pureReactive = new PureReactive(ratePerSecond);

        // This is sets up the `subscribe` callbacks for our async run method and then moves onto the next line of code.
        final Disposable subscription = pureReactive.run()
                .subscribe(unused -> {
                }, error -> {
                    System.err.println("Error occurred: " + error);
                });

        try {
            TimeUnit.SECONDS.sleep(durationInSeconds);
        } catch (InterruptedException e) {
            System.err.println("Error occurred while sleeping thread. " + e);
        } finally {
            System.out.println("Disposing of resources.");

            // Dispose of the subscription, so it stops publishing at an interval of 1s.
            subscription.dispose();

            // Dispose of the logging scheduler we created and dispose of producer client.
            pureReactive.close();
        }

        System.out.printf("Input rate: %d records/s. Actual: %d records/s%n",
                ratePerSecond, pureReactive.getRecordNumber() / durationInSeconds);
        System.out.println("All done.");
    }

    private Mono<Void> run() {
        this.loggerScheduler.scheduleAtFixedRate(this::measureMemoryUsed, 1, 1, TimeUnit.SECONDS);

        // Every 1 second, try to publish `ratePerSecond` number of messages.
        return Flux.interval(Duration.ofSeconds(1))
                .flatMap(index -> publishMessages(index))
                .then(Mono.fromRunnable(() -> {
                    System.out.println("FOO");
                    // Print out final memory usage and number of records published.
                    measureMemoryUsed();
                }));
    }

    int getRecordNumber() {
        return recordNumber.get();
    }

    /**
     * Publishes {@link #ratePerSecond} number of messages.
     *
     * @param iteration The current iteration.
     *
     * @return A Mono that completes when the messages have been published.
     */
    private Mono<Void> publishMessages(long iteration) {
        final Stream<EventData> eventDataStream = IntStream.range(0, ratePerSecond)
                .mapToObj(eventNumber -> {
                    final String contents = String.format("iteration: %d. index: %d. # of records: %d.",
                            iteration, eventNumber, recordNumber.incrementAndGet());

                    return new EventData(contents);
                });

        return Flux.fromStream(eventDataStream)
                .flatMap(eventData -> {
                    return producerAsyncClient.createBatch()
                            .flatMap(eventDataBatch -> {
                                // Should probably check to see if it fits and error if it does not.
                                eventDataBatch.tryAdd(eventData);
                                return producerAsyncClient.send(eventDataBatch);
                            });
                })
                .then();
    }

    private void measureMemoryUsed() {
        long kUsed = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024;
        long megUsed = kUsed / 1024;
        long elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000;
        System.out.println(elapsedSeconds + "s: After " + recordNumber.get() + " records, memory used = " + megUsed + "MB");
    }

    @Override
    public void close() {
        loggerScheduler.shutdown();
        producerAsyncClient.close();
    }
}

