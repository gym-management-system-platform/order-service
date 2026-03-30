package com.order_service.controller.response;


import io.swagger.v3.oas.annotations.media.Schema;

import java.time.LocalDateTime;


@Schema(description = "Структура ответа с информацией об ошибке")
public record ErrorResponse(

        @Schema(description = "Время возникновения ошибки", example = "2025-07-14T10:00:00")
        LocalDateTime timestamp,

        @Schema(description = "Http статус ошибки", example = "400")
        int status,

        @Schema(description = "Краткое описание ошибки", example = "Bad Request")
        String error,

        @Schema(description = "Путь", example = "/api/v1/exercises/10")
        String path
) {
}
