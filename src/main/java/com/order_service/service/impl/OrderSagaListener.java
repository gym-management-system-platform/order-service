package com.order_service.service.impl;


import com.order_service.enums.OrderStatus;
import com.order_service.event.inventory.InventoryEvent;
import com.order_service.event.inventory.InventoryReserveFailedEvent;
import com.order_service.event.inventory.InventoryReservedEvent;
import com.order_service.event.payment.PaymentFailedEvent;
import com.order_service.event.payment.PaymentSucceededEvent;
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
    private final OrderServiceImpl orderServiceImpl;
    private final TransactionalOperator transactionalOperator;

    @KafkaListener(
            topics = "${app.kafka.topics.inventory-events:inventory-events}",
            groupId = "order-service-group",
            containerFactory = "inventoryEventsKafkaListenerContainerFactory"
    )
    public void handleInventoryEvents(InventoryEvent event) {

        Mono<Void> pipeline;
        if (event instanceof InventoryReserveFailedEvent failed) {
            pipeline = handleInventoryReserveFailed(failed);
        } else if (event instanceof InventoryReservedEvent reserved) {
            pipeline = handleInventoryReserved(reserved)
                    .then(orderServiceImpl.startPayment(reserved));
        } else {
            log.warn("Unsupported inventory event type: {}", event != null ? event.getClass() : null);
            return;
        }

        pipeline.subscribe(
                null,
                ex -> log.error("Error processing inventory event", ex)
        );
    }


    /**
     * Инвентарь зарезервирован — переводим заказ в PROCESSING_PAYMENT (следующий шаг саги: оплата).
     */
    private Mono<Void> handleInventoryReserved(InventoryReservedEvent event) {
        return repository.findBySagaId(event.getSagaId())
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
        return repository.findBySagaId(event.getSagaId())
                .doOnNext(o -> log.info("Order found {}", o.getId()))
                .doOnSuccess(o -> log.info("Result {}", o))
                .flatMap(order -> {
                    log.info("Updating order {}", order.getId());
                    order.setStatus(OrderStatus.FAILED);
                    order.setFailureReason("Inventory reservation failed: " + event.getReason());
                    return repository.save(order);
                })
                .as(transactionalOperator::transactional)
                .then();
    }

    @KafkaListener(
            topics = "${app.kafka.topics.payment-succeeded:payment-succeeded}",
            groupId = "order-service-group",
            containerFactory = "paymentSucceededKafkaListenerContainerFactory"
    )
    public Mono<Void> paymentSuccess(PaymentSucceededEvent event) {
        if (event == null) {
            return Mono.empty();
        }
        return repository.findBySagaId(event.getSagaId())
                .flatMap(order -> {
                    order.setStatus(OrderStatus.COMPLETED);
                    return repository.save(order);
                })
                .as(transactionalOperator::transactional)
                .then()
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
            groupId = "order-service-group",
            containerFactory = "paymentFailedKafkaListenerContainerFactory"
    )
    public Mono<Void> paymentFailed(PaymentFailedEvent event) {
        if (event == null) {
            return Mono.empty();
        }
        return orderServiceImpl.compensateOrder(event.getSagaId(), event.getReason())
                .doOnSuccess(order -> {
                    if (order != null) {
                        log.info("Order compensated for sagaId={}", order.getSagaId());
                    }
                })
                .then()
                .onErrorResume(ex -> {
                    log.error("Failed to process payment-failed / compensation", ex);
                    return Mono.empty();
                });
    }
}
