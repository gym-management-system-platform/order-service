package com.order_service.service.impl;


import com.order_service.controller.request.CreateOrderRequest;
import com.order_service.entity.OrderEntity;
import com.order_service.enums.Currency;
import com.order_service.enums.OrderStatus;
import com.order_service.event.inventory.InventoryReservedEvent;
import com.order_service.repository.OrderRepository;
import com.order_service.service.OrderOutboxService;
import com.order_service.service.OrderService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;


@Service
@RequiredArgsConstructor
public class OrderServiceImpl implements OrderService {

    private final OrderRepository orderRepository;
    private final OrderOutboxService orderOutboxService;
    private final TransactionalOperator transactionalOperator;

    @Override
    public Mono<OrderEntity> createOrder(CreateOrderRequest request) {
        Instant now = Instant.now();

        OrderEntity order = OrderEntity.builder()
                .orderNumber(generateNumber())
                .userId(request.userId())
                .amount(request.amount())
                .productId(request.productId())
                .quantity(request.quantity() != null ? request.quantity() : 1)
                .status(OrderStatus.RESERVING_INVENTORY)
                .currency(Currency.RUB)
                .sagaId(UUID.randomUUID())
                .createdAt(now)
                .updatedAt(now)
                .build();

        return orderRepository.save(order)
                .flatMap(savedOrder -> orderOutboxService.enqueueOrderCreated(savedOrder)
                        .thenReturn(savedOrder))
                .as(transactionalOperator::transactional);
    }

    private String generateNumber() {
        return "ORD-" + System.currentTimeMillis();
    }

    /**
     * Компенсирующая транзакция саги: при откате (например, оплата не прошла после резерва инвентаря)
     */
    @Override
    public Mono<OrderEntity> compensateOrder(UUID sagaId, String reason) {
        return orderRepository.findBySagaId(sagaId)
                .flatMap(order -> {
                    order.setStatus(OrderStatus.CANCELLED);
                    order.setFailureReason(reason);

                    return orderRepository.save(order)
                            .then(orderOutboxService.enqueueOrderCompensated(order, reason))
                            .thenReturn(order);
                })
                .as(transactionalOperator::transactional);
    }

    @Override
    public Mono<Void> startPayment(InventoryReservedEvent event) {
        return orderRepository.findBySagaId(event.getSagaId())
                .flatMap(orderOutboxService::enqueueOrderProcessingPayment);
    }

    @Override
    public Mono<OrderEntity> completeOrder(UUID sagaId) {
        return orderRepository.findBySagaId(sagaId)
                .flatMap(order -> {
                    order.setStatus(OrderStatus.COMPLETED);
                    order.setUpdatedAt(Instant.now());
                    return orderRepository.save(order);
                })
                .as(transactionalOperator::transactional);
    }

    @Override
    public Mono<OrderEntity> cancelAfterCompensation(UUID sagaId) {

        return orderRepository.findBySagaId(sagaId)
                .flatMap(order -> {
                    order.setStatus(OrderStatus.CANCELLED);
                    order.setUpdatedAt(Instant.now());
                    return orderRepository.save(order);
                })
                .as(transactionalOperator::transactional);
    }

    @Override
    public Mono<OrderEntity> getOrderById(UUID guid) {
        return orderRepository.findById(guid);
    }

    @Override
    public Mono<OrderEntity> findBySagaId(UUID sagaId) {
        return orderRepository.findBySagaId(sagaId);
    }
}
