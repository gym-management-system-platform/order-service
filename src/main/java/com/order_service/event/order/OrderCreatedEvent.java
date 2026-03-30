package com.order_service.event.order;


import lombok.Getter;
import lombok.experimental.SuperBuilder;

import java.math.BigDecimal;


@Getter
@SuperBuilder
public class OrderCreatedEvent extends OrderEvent {
    private final BigDecimal amount;
    private final String productId;
    private final Integer quantity;
}
