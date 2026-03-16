package com.order_service.event;

import java.time.Instant;


public record InventoryReserveFailedEvent(
        String sagaId,
        String orderId,
        Instant createdAt,
        String productId,
        Integer quantity,
        String reason
) {}