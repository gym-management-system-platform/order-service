package com.order_service.service;


import com.order_service.controller.request.CreateOrderRequest;
import com.order_service.entity.OrderEntity;
import com.order_service.event.inventory.InventoryReservedEvent;
import reactor.core.publisher.Mono;

import java.util.UUID;


/**
 * Сервис для управления заказами
 */
public interface OrderService {

    /**
     * Создание нового заказа
     */
    Mono<OrderEntity> createOrder(CreateOrderRequest request);

    /**
     * Компенсация заказа (отмена с причиной)
     */
    Mono<OrderEntity> compensateOrder(String sagaId, String reason);

    /**
     * Начать процесс оплаты после резерва инвентаря
     */
    Mono<Void> startPayment(InventoryReservedEvent event);

    /**
     * Завершить заказ (успешная оплата)
     */
    Mono<OrderEntity> completeOrder(String sagaId);

    /**
     * Отменить заказ после компенсации
     */
    Mono<OrderEntity> cancelAfterCompensation(String sagaId);

    /**
     * Получить заказ по ID
     */
    Mono<OrderEntity> getOrderById(UUID id);

    /**
     * Найти заказ по sagaId
     */
    Mono<OrderEntity> findBySagaId(String sagaId);
}

