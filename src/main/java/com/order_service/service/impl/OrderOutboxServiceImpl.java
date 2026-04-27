package com.order_service.service.impl;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.order_service.entity.OrderEntity;
import com.order_service.entity.OutboxEventEntity;
import com.order_service.enums.OutboxAggregateType;
import com.order_service.enums.OutboxEventStatus;
import com.order_service.enums.OutboxEventType;
import com.order_service.event.order.OrderCompensatedEvent;
import com.order_service.event.order.OrderCreatedEvent;
import com.order_service.event.order.OrderProcessingPaymentEvent;
import com.order_service.repository.OutboxRepository;
import com.order_service.service.OrderOutboxService;
import io.r2dbc.postgresql.codec.Json;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;


@Service
@RequiredArgsConstructor
public class OrderOutboxServiceImpl implements OrderOutboxService {

    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    @Override
    public Mono<Void> enqueueOrderCreated(OrderEntity order) {
        OrderCreatedEvent event = buildOrderCreatedEvent(order);
        OutboxEventEntity outbox = OutboxEventEntity.builder()
                .aggregateType(OutboxAggregateType.ORDER)
                .aggregateId(order.getId())
                .eventType(OutboxEventType.OrderCreatedEvent)
                .payload(toJson(event))
                .sagaId(order.getSagaId())
                .status(OutboxEventStatus.NEW)
                .createdAt(Instant.now())
                .retryCount(0)
                .build();
        return outboxRepository.save(outbox).then();
    }

    @Override
    public Mono<Void> enqueueOrderCompensated(OrderEntity order, String reason) {
        OrderCompensatedEvent event = OrderCompensatedEvent.builder()
                .sagaId(order.getSagaId())
                .orderId(order.getId())
                .createdAt(Instant.now())
                .reason(reason)
                .build();

        OutboxEventEntity outbox = OutboxEventEntity.builder()
                .aggregateType(OutboxAggregateType.ORDER)
                .aggregateId(order.getId())
                .eventType(OutboxEventType.OrderCompensatedEvent)
                .payload(toJson(event))
                .sagaId(order.getSagaId())
                .status(OutboxEventStatus.NEW)
                .createdAt(Instant.now())
                .retryCount(0)
                .build();
        return outboxRepository.save(outbox).then();
    }

    @Override
    public Mono<Void> enqueueOrderProcessingPayment(OrderEntity order) {
        OrderProcessingPaymentEvent paymentEvent = OrderProcessingPaymentEvent.builder()
                .sagaId(order.getSagaId())
                .orderId(order.getId())
                .amount(order.getAmount())
                .currency(order.getCurrency())
                .createdAt(Instant.now())
                .build();

        OutboxEventEntity outbox = OutboxEventEntity.builder()
                .aggregateType(OutboxAggregateType.ORDER)
                .aggregateId(order.getId())
                .eventType(OutboxEventType.OrderProcessingPaymentEvent)
                .payload(toJson(paymentEvent))
                .sagaId(order.getSagaId())
                .status(OutboxEventStatus.NEW)
                .createdAt(Instant.now())
                .retryCount(0)
                .build();
        return outboxRepository.save(outbox).then();
    }

    private OrderCreatedEvent buildOrderCreatedEvent(OrderEntity order) {
        return OrderCreatedEvent.builder()
                .sagaId(order.getSagaId())
                .orderId(order.getId())
                .createdAt(Instant.now())
                .amount(order.getAmount())
                .productId(order.getProductId())
                .quantity(order.getQuantity())
                .build();
    }

    private Json toJson(Object domainEvent) {
        try {
            return Json.of(objectMapper.writeValueAsString(domainEvent));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Не удалось сериализовать событие для outbox", e);
        }
    }
}
