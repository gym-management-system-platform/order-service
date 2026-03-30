package com.order_service.controller.request;


import java.math.BigDecimal;
import java.util.UUID;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.*;


@Schema(description = "Запрос на создание заказа")
public record CreateOrderRequest(

        @Schema(description = "ID пользователя", example = "550e8400-e29b-41d4-a716-446655440000")
        @NotNull(message = "userId обязателен")
        UUID userId,

        @Schema(description = "Сумма заказа", example = "1999.99")
        @NotNull(message = "amount обязателен")
        @Positive(message = "Сумма должна быть больше 0")
        BigDecimal amount,

        @Schema(description = "ID продукта", example = "550e8400-e29b-41d4-a716-446655440001")
        @NotNull(message = "productId обязателен")
        UUID productId,

        @Schema(description = "Количество товара", example = "2")
        @NotNull(message = "quantity обязателен")
        @Min(value = 1, message = "Минимум 1 товар")
        Integer quantity
) {
}
