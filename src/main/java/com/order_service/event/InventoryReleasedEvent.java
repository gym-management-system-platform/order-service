package com.order_service.event;


import java.time.Instant;


public record InventoryReleasedEvent(
        String sagaId,
        String orderId,
        String productId,
        Integer quantity,
        Instant createdAt
) {
}
