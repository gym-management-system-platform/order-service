package com.order_service.controller;


import com.order_service.controller.request.CreateOrderRequest;
import com.order_service.controller.response.OrderResponse;
import com.order_service.mapper.OrderMapper;
import com.order_service.service.impl.OrderServiceImpl;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.swagger.v3.oas.annotations.responses.*;
import io.swagger.v3.oas.annotations.media.*;
import jakarta.validation.Valid;

import java.util.UUID;


@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
@Tag(name = "Orders", description = "API для работы с заказами")
public class OrderController {

    private final OrderServiceImpl service;
    private final OrderMapper mapper;

    @Operation(
            summary = "Создать заказ",
            description = "Создает новый заказ для пользователя"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Заказ успешно создан",
                    content = @Content(schema = @Schema(implementation = OrderResponse.class))),
            @ApiResponse(responseCode = "400", description = "Ошибка валидации запроса"),
            @ApiResponse(responseCode = "500", description = "Внутренняя ошибка сервера")
    })
    @PostMapping
    public Mono<OrderResponse> create(
            @Valid
            @RequestBody
            @io.swagger.v3.oas.annotations.parameters.RequestBody(
                    description = "Данные для создания заказа",
                    required = true,
                    content = @Content(schema = @Schema(implementation = CreateOrderRequest.class))
            )
            CreateOrderRequest request) {

        return service.createOrder(request)
                .map(mapper::toResponse);
    }

    @Operation(
            summary = "Получить заказ по ID",
            description = "Возвращает информацию о заказе по его ID"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Заказ найден",
                    content = @Content(schema = @Schema(implementation = OrderResponse.class))),
            @ApiResponse(responseCode = "404", description = "Заказ не найден"),
            @ApiResponse(responseCode = "400", description = "Некорректный UUID")
    })
    @GetMapping("/{id}")
    public Mono<OrderResponse> getById(
            @Parameter(description = "ID заказа", required = true,
                    example = "550e8400-e29b-41d4-a716-446655440010")
            @PathVariable UUID id) {

        return service.getOrderById(id)
                .map(mapper::toResponse);
    }
}