package com.order_service.event;


import java.time.Instant;


public record PaymentFailedEvent(
        String sagaId,
        String orderId,
        Instant createdAt,
        String reason
) {}
