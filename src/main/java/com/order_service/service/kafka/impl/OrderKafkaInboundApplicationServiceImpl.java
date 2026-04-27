package com.order_service.service.kafka.impl;


import com.order_service.event.inventory.InventoryEvent;
import com.order_service.event.inventory.InventoryReleasedEvent;
import com.order_service.event.payment.PaymentFailedEvent;
import com.order_service.event.payment.PaymentSucceededEvent;
import com.order_service.inventory.handler.InventoryEventHandlerRegistry;
import com.order_service.service.OrderService;
import com.order_service.service.kafka.OrderKafkaInboundApplicationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;


@Service
@RequiredArgsConstructor
@Slf4j
public class OrderKafkaInboundApplicationServiceImpl implements OrderKafkaInboundApplicationService {

    private final OrderService orderService;
    private final InventoryEventHandlerRegistry inventoryEventHandlerRegistry;

    @Override
    public Mono<Void> onInventoryEvent(InventoryEvent event) {
        return inventoryEventHandlerRegistry.dispatch(event)
                .orElseGet(() -> {
                    log.warn("Неподдерживаемый тип события инвентаря: {}", event.getClass());
                    return Mono.empty();
                });
    }

    @Override
    public Mono<Void> onPaymentSucceeded(PaymentSucceededEvent event) {
        return orderService.completeOrder(event.getSagaId())
                .then()
                .onErrorResume(ex -> {
                    log.error("Ошибка обработки payment-succeeded", ex);
                    return Mono.empty();
                });
    }

    @Override
    public Mono<Void> onPaymentFailed(PaymentFailedEvent event) {
        return orderService.compensateOrder(event.getSagaId(), event.getReason())
                .doOnSuccess(order -> {
                    if (order != null) {
                        log.info("Заказ скомпенсирован, sagaId={}", order.getSagaId());
                    }
                })
                .then()
                .onErrorResume(ex -> {
                    log.error("Ошибка обработки payment-failed / компенсации", ex);
                    return Mono.empty();
                });
    }

    @Override
    public Mono<Void> onInventoryReleased(InventoryReleasedEvent event) {
        return orderService.cancelAfterCompensation(event.getSagaId())
                .then()
                .onErrorResume(ex -> {
                    log.error("Ошибка обработки inventory-released", ex);
                    return Mono.empty();
                });
    }
}
