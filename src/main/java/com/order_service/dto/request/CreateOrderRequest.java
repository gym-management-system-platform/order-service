package com.order_service.dto.request;


import java.math.BigDecimal;
import java.util.UUID;


public record CreateOrderRequest(
        UUID userId,
        BigDecimal amount,
        UUID productId,
        Integer quantity
) {
}
