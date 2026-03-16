package com.order_service.service;


import com.order_service.dto.request.CreateOrderRequest;
import com.order_service.entity.OrderEntity;
import com.order_service.entity.OutboxEventEntity;
import com.order_service.enums.*;
import com.order_service.event.InventoryReservedEvent;
import com.order_service.event.OrderCompensatedEvent;
import com.order_service.event.OrderCreatedEvent;
import com.order_service.event.OrderProcessingPaymentEvent;
import com.order_service.repository.OrderRepository;
import com.order_service.repository.OutboxRepository;
import com.order_service.serialize.EventSerializer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;


@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository repository;
    private final OutboxRepository outboxRepository;
    private final EventSerializer serializer;
    private final TransactionalOperator transactionalOperator;
    private final EventSerializer eventSerializer;

    public Mono<OrderEntity> createOrder(CreateOrderRequest request) {
        String sagaId = UUID.randomUUID().toString();
        Instant now = Instant.now();

        OrderEntity order = OrderEntity.builder()
                .orderNumber(generateNumber())
                .userId(request.userId())
                .amount(request.amount())
                .productId(request.productId())
                .quantity(request.quantity() != null ? request.quantity() : 1)
                .status(OrderStatus.PENDING)
                .currency(Currency.RUB)
                .sagaId(sagaId)
                .createdAt(now)
                .updatedAt(now)
                .build();

        return repository.save(order)
                .flatMap(savedOrder -> {
                    OrderCreatedEvent event = buildEvent(savedOrder);

                    OutboxEventEntity outbox = OutboxEventEntity.builder()
                            .aggregateType(OutboxAggregateType.ORDER)
                            .aggregateId(savedOrder.getId().toString())
                            .eventType(OutboxEventType.OrderCreatedEvent)
                            .payload(serializer.toJson(event))
                            .sagaId(savedOrder.getSagaId())
                            .status(OutboxEventStatus.NEW.name())
                            .createdAt(Instant.now())
                            .retryCount(0)
                            .build();

                    return outboxRepository.save(outbox)
                            .thenReturn(savedOrder);
                })
                .as(transactionalOperator::transactional);
    }


    private String generateNumber() {
        return "ORD-" + System.currentTimeMillis();
    }

    /**
     * Компенсирующая транзакция саги: при откате (например, оплата не прошла после резерва инвентаря)
     * обновляем заказ и записываем OrderCompensatedEvent в outbox для inventory-service (освобождение резерва).
     */
    public Mono<OrderEntity> compensateOrder(String sagaId, String reason) {
        return repository.findBySagaId(sagaId)
                .flatMap(order -> {
                    order.setStatus(OrderStatus.CANCELLED);
                    order.setFailureReason(reason);

                    OrderCompensatedEvent event = new OrderCompensatedEvent(
                            sagaId,
                            order.getId().toString(),
                            Instant.now(),
                            reason);

                    OutboxEventEntity outbox = OutboxEventEntity.builder()
                            .id(UUID.randomUUID())
                            .aggregateType(OutboxAggregateType.ORDER)
                            .aggregateId(order.getId().toString())
                            .eventType(OutboxEventType.OrderCompensatedEvent)
                            .payload(serializer.toJson(event))
                            .sagaId(sagaId)
                            .status(OutboxEventStatus.NEW.name())
                            .createdAt(Instant.now())
                            .retryCount(0)
                            .build();

                    return repository.save(order)
                            .then(outboxRepository.save(outbox))
                            .thenReturn(order);
                })
                .as(transactionalOperator::transactional);
    }

    private OrderCreatedEvent buildEvent(OrderEntity order) {
        return new OrderCreatedEvent(
                order.getSagaId(),
                order.getId().toString(),
                Instant.now(),
                order.getAmount(),
                order.getProductId() != null ? order.getProductId().toString() : null,
                order.getQuantity()

        );
    }

    public Mono<Void> startPayment(InventoryReservedEvent event) {
        return repository.findBySagaId(event.sagaId())
                .flatMap(order -> {
                    OrderProcessingPaymentEvent paymentEvent =
                            new OrderProcessingPaymentEvent(
                                    order.getSagaId(),
                                    order.getId().toString(),
                                    order.getAmount(),
                                    order.getCurrency().name(),
                                    Instant.now()
                            );

                    OutboxEventEntity outbox = OutboxEventEntity.builder()
                            .aggregateType(OutboxAggregateType.ORDER)
                            .aggregateId(order.getId().toString())
                            .eventType(OutboxEventType.OrderProcessingPaymentEvent)
                            .payload(eventSerializer.toJson(paymentEvent))
                            .sagaId(order.getSagaId())
                            .status(OutboxEventStatus.NEW.name())
                            .createdAt(Instant.now())
                            .retryCount(0)
                            .build();

                    return outboxRepository.save(outbox).then();
                });
    }

    public Mono<OrderEntity> completeOrder(String sagaId) {

        return repository.findBySagaId(sagaId)
                .flatMap(order -> {
                    order.setStatus(OrderStatus.COMPLETED);
                    order.setUpdatedAt(Instant.now());
                    return repository.save(order);

                })
                .as(transactionalOperator::transactional);
    }

    public Mono<OrderEntity> cancelAfterCompensation(String sagaId) {

        return repository.findBySagaId(sagaId)
                .flatMap(order -> {

                    order.setStatus(OrderStatus.CANCELLED);
                    order.setUpdatedAt(Instant.now());

                    return repository.save(order);

                })
                .as(transactionalOperator::transactional);
    }

    public Mono<OrderEntity> getOrderById(UUID guid) {
        return repository.findById(guid);
    }
}
