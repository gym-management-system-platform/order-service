package com.order_service.event;


import java.math.BigDecimal;
import java.time.Instant;


public record OrderCreatedEvent(
        String sagaId,
        String orderId,
        Instant createdAt,
        BigDecimal amount,
        String productId,
        Integer quantity
) {
}
