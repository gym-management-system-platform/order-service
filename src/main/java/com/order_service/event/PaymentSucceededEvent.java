package com.order_service.event;


import java.time.Instant;


public record PaymentSucceededEvent(
        String sagaId,
        String orderId,
        Instant createdAt,
        String paymentId
) {
}
