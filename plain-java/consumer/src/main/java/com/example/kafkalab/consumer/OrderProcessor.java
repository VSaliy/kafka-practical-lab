package com.example.kafkalab.consumer;

import com.example.kafkalab.common.model.OrderCreated;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderProcessor {

    private static final Logger log = LoggerFactory.getLogger(OrderProcessor.class);
    private static final Random RANDOM = new Random();

    private final long processingDelayMs;
    private final double failureRate;

    public OrderProcessor(long processingDelayMs, double failureRate) {
        this.processingDelayMs = processingDelayMs;
        this.failureRate = failureRate;
    }

    public void process(OrderCreated order) {
        log.info("Processing order: id={}, customer={}, amount={} {}",
            order.orderId(), order.customerId(), order.amount(), order.currency());

        simulateDelay();
        simulateFailure(order);

        log.info("Successfully processed order: {}", order.orderId());
    }

    private void simulateDelay() {
        if (processingDelayMs > 0) {
            try {
                Thread.sleep(processingDelayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ProcessingException("Interrupted during processing delay", e);
            }
        }
    }

    private void simulateFailure(OrderCreated order) {
        if (failureRate > 0 && RANDOM.nextDouble() < failureRate) {
            throw new ProcessingException("Simulated processing failure for order: " + order.orderId());
        }
    }
}
