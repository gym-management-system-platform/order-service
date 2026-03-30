package com.order_service.service.impl;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.order_service.controller.request.CreateOrderRequest;
import com.order_service.entity.OrderEntity;
import com.order_service.entity.OutboxEventEntity;
import com.order_service.enums.*;
import com.order_service.event.inventory.InventoryReservedEvent;
import com.order_service.event.order.OrderCompensatedEvent;
import com.order_service.event.order.OrderCreatedEvent;
import com.order_service.event.order.OrderProcessingPaymentEvent;
import com.order_service.repository.OrderRepository;
import com.order_service.repository.OutboxRepository;
import com.order_service.service.OrderService;
import io.r2dbc.postgresql.codec.Json;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;


@Service
@RequiredArgsConstructor
public class OrderServiceImpl implements OrderService {

    private final OrderRepository repository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;
    private final TransactionalOperator transactionalOperator;

    @Override
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
                            .payload(toOutboxPayload(event))
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

    private Json toOutboxPayload(Object domainEvent) {
        try {
            return Json.of(objectMapper.writeValueAsString(domainEvent));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize event for outbox", e);
        }
    }

    /**
     * Компенсирующая транзакция саги: при откате (например, оплата не прошла после резерва инвентаря)
     * обновляем заказ и записываем OrderCompensatedEvent в outbox для inventory-service (освобождение резерва).
     */
    @Override
    public Mono<OrderEntity> compensateOrder(String sagaId, String reason) {
        return repository.findBySagaId(sagaId)
                .flatMap(order -> {
                    order.setStatus(OrderStatus.CANCELLED);
                    order.setFailureReason(reason);

                    OrderCompensatedEvent event = OrderCompensatedEvent.builder()
                            .sagaId(sagaId)
                            .orderId(order.getId().toString())
                            .createdAt(Instant.now())
                            .reason(reason)
                            .build();

                    OutboxEventEntity outbox = OutboxEventEntity.builder()
                            .id(UUID.randomUUID())
                            .aggregateType(OutboxAggregateType.ORDER)
                            .aggregateId(order.getId().toString())
                            .eventType(OutboxEventType.OrderCompensatedEvent)
                            .payload(toOutboxPayload(event))
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
        return OrderCreatedEvent.builder()
                .sagaId(order.getSagaId())
                .orderId(order.getId().toString())
                .createdAt(Instant.now())
                .amount(order.getAmount())
                .productId(order.getProductId() != null
                        ? order.getProductId().toString()
                        : null)
                .quantity(order.getQuantity())
                .build();
    }

    @Override
    public Mono<Void> startPayment(InventoryReservedEvent event) {
        return repository.findBySagaId(event.getSagaId())
                .flatMap(order -> {
                    OrderProcessingPaymentEvent paymentEvent =
                            OrderProcessingPaymentEvent.builder()
                                    .sagaId(order.getSagaId())
                                    .orderId(order.getId().toString())
                                    .amount(order.getAmount())
                                    .currency(order.getCurrency().name())
                                    .createdAt(Instant.now())
                                    .build();

                    OutboxEventEntity outbox = OutboxEventEntity.builder()
                            .aggregateType(OutboxAggregateType.ORDER)
                            .aggregateId(order.getId().toString())
                            .eventType(OutboxEventType.OrderProcessingPaymentEvent)
                            .payload(toOutboxPayload(paymentEvent))
                            .sagaId(order.getSagaId())
                            .status(OutboxEventStatus.NEW.name())
                            .createdAt(Instant.now())
                            .retryCount(0)
                            .build();

                    return outboxRepository.save(outbox).then();
                });
    }

    @Override
    public Mono<OrderEntity> completeOrder(String sagaId) {

        return repository.findBySagaId(sagaId)
                .flatMap(order -> {
                    order.setStatus(OrderStatus.COMPLETED);
                    order.setUpdatedAt(Instant.now());
                    return repository.save(order);

                })
                .as(transactionalOperator::transactional);
    }

    @Override
    public Mono<OrderEntity> cancelAfterCompensation(String sagaId) {

        return repository.findBySagaId(sagaId)
                .flatMap(order -> {

                    order.setStatus(OrderStatus.CANCELLED);
                    order.setUpdatedAt(Instant.now());

                    return repository.save(order);

                })
                .as(transactionalOperator::transactional);
    }

    @Override
    public Mono<OrderEntity> getOrderById(UUID guid) {
        return repository.findById(guid);
    }

    @Override
    public Mono<OrderEntity> findBySagaId(String sagaId) {
        return repository.findBySagaId(sagaId);
    }
}
