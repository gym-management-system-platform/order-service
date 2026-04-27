package com.order_service.service.kafka;

import com.order_service.event.inventory.InventoryEvent;
import com.order_service.event.inventory.InventoryReleasedEvent;
import com.order_service.event.payment.PaymentFailedEvent;
import com.order_service.event.payment.PaymentSucceededEvent;
import reactor.core.publisher.Mono;

/**
 * Вся бизнес-реакция order-service на входящие Kafka-сообщения (без ack / без @KafkaListener).
 */
public interface OrderKafkaInboundApplicationService {

    Mono<Void> onInventoryEvent(InventoryEvent event);

    Mono<Void> onPaymentSucceeded(PaymentSucceededEvent event);

    Mono<Void> onPaymentFailed(PaymentFailedEvent event);

    Mono<Void> onInventoryReleased(InventoryReleasedEvent event);
}
