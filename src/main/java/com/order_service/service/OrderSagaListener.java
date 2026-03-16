package com.order_service.service;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.order_service.enums.OrderStatus;
import com.order_service.event.*;
import com.order_service.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;


@Component
@RequiredArgsConstructor
@Slf4j
public class OrderSagaListener {
    private final OrderRepository repository;
    private final OrderService orderService;
    private final TransactionalOperator transactionalOperator;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "${app.kafka.topics.inventory-events:inventory-events}",
            groupId = "order-service-group"
    )
    public void handleInventoryEvents(String message) {

        try {

            JsonNode node = objectMapper.readTree(message);

            Mono<Void> pipeline;

            if (node.has("reason")) {
                InventoryReserveFailedEvent event =
                        objectMapper.treeToValue(node, InventoryReserveFailedEvent.class);

                pipeline = handleInventoryReserveFailed(event);

            } else {

                InventoryReservedEvent event =
                        objectMapper.treeToValue(node, InventoryReservedEvent.class);

                pipeline = handleInventoryReserved(event)
                        .then(orderService.startPayment(event));
            }

            pipeline.subscribe(
                    null,
                    ex -> log.error("Error processing inventory event", ex)
            );

        } catch (Exception e) {
            log.error("Failed parse inventory event", e);
        }
    }


    /**
     * Инвентарь зарезервирован — переводим заказ в PROCESSING_PAYMENT (следующий шаг саги: оплата).
     */
    private Mono<Void> handleInventoryReserved(InventoryReservedEvent event) {
        return repository.findBySagaId(event.sagaId())
                .flatMap(order -> {
                    if (order.getStatus() == OrderStatus.COMPLETED) {
                        return Mono.empty();
                    }
                    order.setStatus(OrderStatus.PROCESSING_PAYMENT);
                    return repository.save(order).thenReturn(order);
                })
                .as(transactionalOperator::transactional)
                .then();
    }

    private Mono<Void> handleInventoryReserveFailed(InventoryReserveFailedEvent event) {
        return repository.findBySagaId(event.sagaId())
                .doOnNext(o -> log.info("Order found {}", o.getId()))
                .doOnSuccess(o -> log.info("Result {}", o))
                .flatMap(order -> {
                    log.info("Updating order {}", order.getId());
                    order.setStatus(OrderStatus.FAILED);
                    order.setFailureReason("Inventory reservation failed: " + event.reason());
                    return repository.save(order);
                })
                .as(transactionalOperator::transactional)
                .then();
    }

    @KafkaListener(
            topics = "${app.kafka.topics.payment-succeeded:payment-succeeded}",
            groupId = "order-service-group"
    )
    public Mono<Void> paymentSuccess(String message) {
        if (message == null || message.isBlank()) return Mono.empty();
        return parsePaymentSucceeded(message)
                .flatMap(event -> repository.findBySagaId(event.sagaId())
                        .flatMap(order -> {
                            order.setStatus(OrderStatus.COMPLETED);
                            return repository.save(order);
                        })
                        .as(transactionalOperator::transactional)
                        .then())
                .onErrorResume(ex -> {
                    log.error("Failed to process payment-succeeded", ex);
                    return Mono.empty();
                });
    }

    /**
     * Оплата не прошла. Компенсирующая транзакция: обновляем заказ и пишем OrderCompensatedEvent в outbox,
     * чтобы inventory-service освободил резерв.
     */
    @KafkaListener(
            topics = "${app.kafka.topics.payment-failed:payment-failed}",
            groupId = "order-service-group"
    )
    public Mono<Void> paymentFailed(String message) {
        if (message == null || message.isBlank()) return Mono.empty();
        return parsePaymentFailed(message)
                .flatMap(event -> orderService.compensateOrder(event.sagaId(), event.reason()))
                .doOnSuccess(order -> {
                    if (order != null) log.info("Order compensated for sagaId={}", order.getSagaId());
                })
                .then()
                .onErrorResume(ex -> {
                    log.error("Failed to process payment-failed / compensation", ex);
                    return Mono.empty();
                });
    }

    private Mono<PaymentSucceededEvent> parsePaymentSucceeded(String message) {
        return Mono.fromCallable(() -> objectMapper.readValue(message, PaymentSucceededEvent.class))
                .onErrorResume(e -> {
                    log.warn("Could not parse payment-succeeded: {}", message, e);
                    return Mono.empty();
                });
    }

    private Mono<PaymentFailedEvent> parsePaymentFailed(String message) {
        return Mono.fromCallable(() -> objectMapper.readValue(message, PaymentFailedEvent.class))
                .onErrorResume(e -> {
                    log.warn("Could not parse payment-failed: {}", message, e);
                    return Mono.empty();
                });
    }
}