package com.order_service.controller.response;


import io.swagger.v3.oas.annotations.media.Schema;

import java.math.BigDecimal;
import java.util.UUID;


@Schema(description = "Ответ с информацией о заказе")
public record OrderResponse(

        @Schema(description = "ID заказа", example = "550e8400-e29b-41d4-a716-446655440010")
        UUID id,

        @Schema(description = "Номер заказа", example = "ORD-2026-0001")
        String orderNumber,

        @Schema(description = "Статус заказа", example = "CREATED")
        String status,

        @Schema(description = "Сумма заказа", example = "1999.99")
        BigDecimal amount
) {
}
