package com.order_service.service;

import com.order_service.entity.OrderEntity;
import reactor.core.publisher.Mono;

/**
 * Запись доменных событий заказа в transactional outbox (без прямого доступа доменного сервиса к {@link com.order_service.repository.OutboxRepository}).
 */
public interface OrderOutboxService {

    Mono<Void> enqueueOrderCreated(OrderEntity order);

    Mono<Void> enqueueOrderCompensated(OrderEntity order, String reason);

    Mono<Void> enqueueOrderProcessingPayment(OrderEntity order);
}
