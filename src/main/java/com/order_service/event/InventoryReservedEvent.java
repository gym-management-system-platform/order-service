package com.order_service.event;


import java.time.Instant;


public record InventoryReservedEvent(
        String sagaId,
        String orderId,
        Instant createdAt,
        String productId,
        Integer quantity
) {
}