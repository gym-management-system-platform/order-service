package com.order_service.dto.response;


import java.math.BigDecimal;
import java.util.UUID;


public record OrderResponse(
         UUID id,
         String orderNumber,
         String status,
         BigDecimal amount
) {
}
