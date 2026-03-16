package com.order_service.event;


import java.math.BigDecimal;
import java.time.Instant;


public record OrderProcessingPaymentEvent(
        String sagaId,
        String orderId,
        BigDecimal amount,
        String currency,
        Instant createdAt
) {
}
