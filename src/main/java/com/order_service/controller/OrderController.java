package com.order_service.controller;


import com.order_service.dto.request.CreateOrderRequest;
import com.order_service.dto.response.OrderResponse;
import com.order_service.mapper.OrderMapper;
import com.order_service.service.OrderService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.UUID;


@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService service;
    private final OrderMapper mapper;

    @PostMapping
    public Mono<OrderResponse> create(
            @RequestBody CreateOrderRequest request) {

        return service.createOrder(request)
                .map(mapper::toResponse);
    }

    @GetMapping("/{id}")
    public Mono<OrderResponse> getById(@PathVariable UUID id) {
        return service.getOrderById(id).map(mapper::toResponse);

    }
}